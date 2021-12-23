//package org.apache.flink.streaming.connectors.unified.e2e;
//
//
//import lombok.Cleanup;
//import lombok.extern.slf4j.Slf4j;
//
//import org.apache.flink.streaming.connectors.unified.container.DebeziumMysqlContainer;
//import org.apache.flink.streaming.connectors.unified.container.PulsarContainer;
//import org.apache.flink.streaming.connectors.unified.tests.cluster.FunctionRuntimeType;
//
//import org.apache.flink.streaming.connectors.unified.tests.cluster.PulsarClusterSpec;
//
//import org.apache.pulsar.client.admin.PulsarAdmin;
//import org.apache.pulsar.client.api.PulsarClient;
//import org.apache.pulsar.common.naming.TopicName;
//import org.apache.pulsar.common.policies.data.Policies;
//import org.apache.pulsar.common.policies.data.RetentionPolicies;
//import org.apache.pulsar.common.policies.data.TenantInfoImpl;
//import org.apache.pulsar.common.schema.SchemaInfo;
//import org.junit.Test;
//import org.testcontainers.shaded.com.google.common.collect.Sets;
//import org.testcontainers.utility.DockerImageName;
//
//import java.util.concurrent.ThreadLocalRandom;
//import java.util.concurrent.atomic.AtomicInteger;
//import java.util.stream.Stream;
//
//import static java.util.stream.Collectors.joining;
//
//
///**
// * Debezium解析mysql的changelog 到pulsar 端到端测试
// */
//@Slf4j
//public class PulsarDebeziumSourceTests {
//
//    protected final AtomicInteger testId = new AtomicInteger(0);
//
//    protected final FunctionRuntimeType functionRuntimeType = FunctionRuntimeType.PROCESS;
//
//
//    private static final DockerImageName PULSAR_IMAGE = DockerImageName.parse("apachepulsar/pulsar:2.8.0");
//    protected PulsarContainer pulsarCluster;
//
//    private int currentSetupNumber;
//    private int failedSetupNumber = 0;
//
//    protected String clusterName = Stream.of(this.getClass().getSimpleName(), "pulsar-prefix", randomName(5))
//            .filter(s -> s != null && !s.isEmpty())
//            .collect(joining("-"));
//
//
//
//    protected void setupCluster(PulsarClusterSpec spec) throws Exception {
//        incrementSetupNumber();
//        log.info("Setting up cluster {} with {} bookies, {} brokers",
//                spec.clusterName(), spec.numsBookies(), spec.numsBrokers());
//
//        pulsarCluster = new PulsarContainer(PULSAR_IMAGE);
//
//        beforeStartCluster();
//
//        pulsarCluster.start();
//
//        log.info("Cluster {} is setup", spec.clusterName());
//    }
//
//
//    protected void beforeStartCluster() throws Exception {
//        // no-op
//    }
//    protected final void incrementSetupNumber() {
//        currentSetupNumber++;
//        failedSetupNumber = -1;
//        log.debug("currentSetupNumber={}", currentSetupNumber);
//    }
//
////    @Test(groups = "source")
//    public void testDebeziumMySqlSourceJson() throws Exception {
//        testDebeziumMySqlConnect("org.apache.kafka.connect.json.JsonConverter", true);
//    }
//
//
//    private void testDebeziumMySqlConnect(String converterClassName, boolean jsonWithEnvelope) throws Exception {
//
//        final String tenant = TopicName.PUBLIC_TENANT;
//        final String namespace = TopicName.DEFAULT_NAMESPACE;
//        final String outputTopicName = "debe-output-topic-name-" + testId.getAndIncrement();
//        boolean isJsonConverter = converterClassName.endsWith("JsonConverter");
//        final String consumeTopicName = "debezium/mysql-"
//                + (isJsonConverter ? "json" : "avro")
//                + "/dbserver1.inventory.products";
//        final String sourceName = "test-source-debezium-mysql" + (isJsonConverter ? "json" : "avro")
//                + "-" + functionRuntimeType + "-" + randomName(8);
//
//        // This is the binlog count that contained in mysql container.
//        final int numMessages = 47;
//
//        @Cleanup
//        PulsarClient client = PulsarClient.builder()
//                .serviceUrl(pulsarCluster.getPulsarBrokerUrl())
//                .build();
//
//        @Cleanup
//        PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(pulsarCluster.getHttpServiceUrl()).build();
//        initNamespace(admin);
//
//        try {
//            SchemaInfo lastSchemaInfo = admin.schemas().getSchemaInfo(consumeTopicName);
//            log.info("lastSchemaInfo: {}", lastSchemaInfo == null ? "null" : lastSchemaInfo.toString());
//        } catch (Exception e) {
//            log.warn("failed to get schemaInfo for topic: {}, exceptions message: {}",
//                    consumeTopicName, e.getMessage());
//        }
//
//        admin.topics().createNonPartitionedTopic(outputTopicName);
//
//        DebeziumMySqlSourceTest sourceTester = new DebeziumMySqlSourceTest(pulsarCluster, converterClassName);
//        sourceTester.getSourceConfig().put("json-with-envelope", jsonWithEnvelope);
//
//        // setup debezium mysql server
//        DebeziumMysqlContainer mySQLContainer = new DebeziumMysqlContainer(clusterName,"apachepulsar/pulsar");
//        sourceTester.setServiceContainer(mySQLContainer);
//
//
//        //FIXME 从Debezium解析的changelog操作 导入到pulsar
//        // 替换成flink cdc
//        PulsarIODebeziumSourceRunner runner = new PulsarIODebeziumSourceRunner(pulsarCluster, functionRuntimeType.toString(),
//                converterClassName, tenant, namespace, sourceName, outputTopicName, numMessages, jsonWithEnvelope,
//                consumeTopicName, client);
//
//
//
//        runner.testSource(sourceTester);
//    }
//
//    public static String randomName(int numChars) {
//        StringBuilder sb = new StringBuilder();
//        for (int i = 0; i < numChars; i++) {
//            sb.append((char) (ThreadLocalRandom.current().nextInt(26) + 'a'));
//        }
//        return sb.toString();
//    }
//
//
//    protected void initNamespace(PulsarAdmin admin) {
//        log.info("[initNamespace] start.");
//        try {
//            admin.tenants().createTenant("debezium", new TenantInfoImpl(
//                    Sets.newHashSet(),
//                    Sets.newHashSet(clusterName)));
//            String [] namespaces = {
//                    "debezium/mysql-json",
//                    "debezium/mysql-avro",
//                    "debezium/mongodb",
//                    "debezium/postgresql",
//                    "debezium/mssql",
//            };
//            Policies policies = new Policies();
//            policies.retention_policies = new RetentionPolicies(-1, 50);
//            for (String ns: namespaces) {
//                admin.namespaces().createNamespace(ns, policies);
//            }
//        } catch (Exception e) {
//            log.info("[initNamespace] msg: {}", e.getMessage());
//        }
//        log.info("[initNamespace] finish.");
//    }
//
//
//
//}
