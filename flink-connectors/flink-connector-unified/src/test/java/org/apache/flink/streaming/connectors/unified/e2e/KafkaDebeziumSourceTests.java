package org.apache.flink.streaming.connectors.unified.e2e;

import com.jayway.jsonpath.JsonPath;
import io.debezium.testing.testcontainers.ConnectorConfiguration;
import io.debezium.testing.testcontainers.DebeziumContainer;
import lombok.extern.slf4j.Slf4j;

import org.apache.flink.streaming.connectors.unified.container.DebeziumMysqlContainer;
import org.apache.flink.streaming.connectors.unified.container.KafkaContainer;

import org.apache.flink.streaming.connectors.unified.container.MySQLContainer;
import org.apache.flink.streaming.connectors.unified.tests.SourceTester;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;

@Slf4j
public class KafkaDebeziumSourceTests {


//    public static Logger  log = LoggerFactory.getLogger(KafkaDebeziumSourceTests.class);

    private String clusterName;
    private static final DockerImageName KAFKA_TEST_IMAGE = DockerImageName.parse("confluentinc/cp-kafka:6.2.1");
    private static final DockerImageName ZOOKEEPER_TEST_IMAGE = DockerImageName.parse("confluentinc/cp-zookeeper:4.0.0");
    private static final DockerImageName DEBEZIUM_IMAGE = DockerImageName.parse("debezium/example-mysql:1.8.0.Final");
    public static final DockerImageName MYSQL_IMAGE = DockerImageName.parse("arm64v8/mariadb:10.7");

    MySQLContainer mySQLContainer = new MySQLContainer("arm64v8/mariadb:10.7");
    //setup kafka server
    public static  Network network = Network.newNetwork();

    public static GenericContainer<?> zookeeper = new GenericContainer<>(ZOOKEEPER_TEST_IMAGE)
                .withNetwork(network)
                .withNetworkAliases("zookeeper")
                .withEnv("ZOOKEEPER_CLIENT_PORT", "2181");



    public static KafkaContainer kafkaContainer = new KafkaContainer(KAFKA_TEST_IMAGE)
            .withEmbeddedZookeeper()
            .withNetwork(network)
            .withMinimumRunningDuration(Duration.ofMillis(360000000));

    public static DebeziumContainer debeziumContainer = new DebeziumContainer(DEBEZIUM_IMAGE)
            .withNetwork(network)
            .dependsOn(kafkaContainer);

    @Before
    public void setupCluster(){
        clusterName = Stream.of(this.getClass().getSimpleName(), randomName(5))
                .filter(s -> s != null && !s.isEmpty())
                .collect(joining("-"));
    }

    @Test
    public void testtDebeziumMySqlSourceJson() throws Exception{
        testDebeziumMySqlConnect("org.apache.kafka.connect.json.JsonConverter", true);
    }

    @Test
    public void testKafkaStart(){

        try (
            KafkaContainer kafkaContainer = new KafkaContainer(KAFKA_TEST_IMAGE)
                    .withEmbeddedZookeeper()
                    .withNetwork(network);

            GenericContainer<?> zookeeper = new GenericContainer<>(ZOOKEEPER_TEST_IMAGE)
                    .withNetwork(network)
                    .withNetworkAliases("zookeeper")
                    .withEnv("ZOOKEEPER_CLIENT_PORT", "2181");
            GenericContainer<?> application = new GenericContainer<>(DockerImageName.parse("alpine"))
                    .withNetwork(network)
                    // }
                    .withNetworkAliases("dummy")
                    .withCommand("sleep 10000")

                    )
        {
            zookeeper.start();
            kafkaContainer.start();
            application.start();
        }



    }

    public void testDebeziumMySqlConnect(String converterClassName, boolean jsonWithEnvelope) throws Exception {


        KafkaContainer kafkaContainer = new KafkaContainer(KAFKA_TEST_IMAGE)
                .withEmbeddedZookeeper()
                .withNetwork(network);

        GenericContainer<?> zookeeper = new GenericContainer<>(ZOOKEEPER_TEST_IMAGE)
                .withNetwork(network)
                .withNetworkAliases("zookeeper")
                .withEnv("ZOOKEEPER_CLIENT_PORT", "2181");
        GenericContainer<?> application = new GenericContainer<>(DockerImageName.parse("alpine"))
                .withNetwork(network)
                // }
                .withNetworkAliases("dummy")
                .withCommand("sleep 10000");
        zookeeper.start();
        kafkaContainer.start();
        application.start();


        //setup mysql server and init database
        MySQLContainer mySQLContainer = new MySQLContainer(MYSQL_IMAGE)
                .withConfigurationOverride("docker.mysql/my.cnf")
                .withSetupSQL("docker.mysql/setup.sql")
                .withUsername("root")
                .withPassword("knxy0616")
                .withDatabaseName("inventory");
        mySQLContainer.start();

        // setup unit test
        DebeziumMySqlByKafkaSourceTest sourceTester = new DebeziumMySqlByKafkaSourceTest(
                kafkaContainer,
                converterClassName);
        sourceTester.getSourceConfig().put("json-with-envelope", jsonWithEnvelope);

        // setup debezium mysql connector server
        DebeziumMysqlContainer debeziumMysqlContainer = new DebeziumMysqlContainer(clusterName);
        sourceTester.setServiceContainer(debeziumMysqlContainer);


        //FIXME 从Debezium解析的changelog操作 导入到kafka
        // 替换成flink cdc


        final int numEntriesToInsert = sourceTester.getNumEntriesToInsert();
        for (int i = 1; i <= numEntriesToInsert; i++) {
            // prepare insert event
            sourceTester.prepareInsertEvent();
            log.info("inserted entry {} of {}", i, numEntriesToInsert);
            // validate the source insert event
            sourceTester.validateSourceResult(1, SourceTester.INSERT, converterClassName);
        }

        // prepare update event
        sourceTester.prepareUpdateEvent();

        // validate the source update event
        sourceTester.validateSourceResult(numEntriesToInsert, SourceTester.UPDATE, converterClassName);

        // prepare delete event
        sourceTester.prepareDeleteEvent();




    }


    @Test
    public void canRegisterPostgreSqlConnector() throws Exception {

        //因为本机是m1 需要交叉编译 所以后面推上去需要替换成 mysql:8.0

        mySQLContainer
                .withConfigurationOverride("docker.mysql/mysql/my.cnf")
                .withSetupSQL("docker.mysql/mysql/setup.sql")
                .withUsername("root")
                .withPassword("knxy0616")
                .withDatabaseName("inventory");
        mySQLContainer.start();
        KafkaContainer kafkaContainer = new KafkaContainer(KAFKA_TEST_IMAGE);
        kafkaContainer.start();

        try (Connection connection = getConnection(mySQLContainer);
             Statement statement = connection.createStatement();
             KafkaConsumer<String, String> consumer = getConsumer(
                     kafkaContainer)) {

            statement.execute("create schema todo");
            statement.execute("create table todo.Todo (id int8 not null, " +
                    "title varchar(255), primary key (id))");
            statement.execute("alter table todo.Todo replica identity full");
            statement.execute("insert into todo.Todo values (1, " +
                    "'Learn CDC')");
            statement.execute("insert into todo.Todo values (2, " +
                    "'Learn Debezium')");

            ConnectorConfiguration connector = ConnectorConfiguration
                    .forJdbcContainer(mySQLContainer)
                    .with("database.server.name", "dbserver1")
                    .with("database.hostname", DebeziumMysqlContainer.NAME)
                    .with("database.port", "3306")
                    .with("database.user", "windwheel")
                    .with("database.password", "knxy0616")
                    .with("database.server.id", "184054")
                    .with("database.server.name", "dbserver1")
                    .with("database.whitelist", "inventory")
                    .with("database.history.kafka.bootstrap.servers",kafkaContainer.getBootstrapServers())
                    .with("database.history.kafka.topic","dbserver1.todo.todo");


            debeziumContainer.registerConnector("my-connector",
                    connector);


            consumer.subscribe(Arrays.asList("dbserver1.todo.todo"));

            List<ConsumerRecord<String, String>> changeEvents =
                    drain(consumer, 2);

            Assert.assertEquals(JsonPath.<Integer> read(changeEvents.get(0).key(),
                    "$.id").intValue(),1);
            Assert.assertEquals(JsonPath.<String> read(changeEvents.get(0).value(),
                    "$.op"),"r");
            Assert.assertEquals(JsonPath.<String> read(changeEvents.get(0).value(),
                    "$.after.title"),"Learn CDC");

            Assert.assertEquals(JsonPath.<Integer> read(changeEvents.get(1).key(),
                    "$.id").intValue(),2);
            Assert.assertEquals(JsonPath.<String> read(changeEvents.get(1).value(),
                    "$.op"),"r");
            Assert.assertEquals(JsonPath.<String> read(changeEvents.get(1).value(),
                    "$.after.title"),"Learn Debezium");

            consumer.unsubscribe();
        }
    }

    // Helper methods below

    private Connection getConnection(
            MySQLContainer postgresContainer)
            throws SQLException {

        return DriverManager.getConnection(postgresContainer.getJdbcUrl(),
                postgresContainer.getUsername(),
                postgresContainer.getPassword());
    }

    private KafkaConsumer<String, String> getConsumer(
            KafkaContainer kafkaContainer) {

        return new KafkaConsumer<String,String>(

                ImmutableMap.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                        kafkaContainer.getBootstrapServers(),
                        ConsumerConfig.GROUP_ID_CONFIG,
                        "tc-" + UUID.randomUUID(),
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                        "earliest"),
                new StringDeserializer(),
                new StringDeserializer());
    }

    private List<ConsumerRecord<String, String>> drain(
            KafkaConsumer<String, String> consumer,
            int expectedRecordCount) {

        List<ConsumerRecord<String, String>> allRecords = new ArrayList<>();

        Unreliables.retryUntilTrue(10, TimeUnit.SECONDS, () -> {
            consumer.poll(Duration.ofMillis(50))
                    .iterator()
                    .forEachRemaining(allRecords::add);

            return allRecords.size() == expectedRecordCount;
        });

        return allRecords;
    }

    public static String randomName(int numChars) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < numChars; i++) {
            sb.append((char) (ThreadLocalRandom.current().nextInt(26) + 'a'));
        }
        return sb.toString();
    }


}
