package org.apache.flink.streaming.connectors.unified.e2e;

import lombok.Getter;

import lombok.extern.slf4j.Slf4j;

import org.apache.flink.streaming.connectors.unified.container.DebeziumMysqlContainer;
import org.apache.flink.streaming.connectors.unified.container.KafkaContainer;
import org.apache.flink.streaming.connectors.unified.container.PulsarContainer;
import org.apache.flink.streaming.connectors.unified.tests.SourceTester;

import java.util.Map;

@Slf4j
@Getter
public class DebeziumMySqlByKafkaSourceTest extends SourceTester<DebeziumMysqlContainer> {


    private static final String NAME = "debezium-mysql";

    private  String kafkaServiceUrl;

    private DebeziumMysqlContainer debeziumMysqlContainer;

    private  KafkaContainer kafkaCluster;




    protected DebeziumMySqlByKafkaSourceTest(KafkaContainer cluster,String converterClassName) {
        super(NAME);
        this.kafkaCluster = cluster;
        kafkaServiceUrl = cluster.getBootstrapServers();

        sourceConfig.put("database.hostname", DebeziumMysqlContainer.NAME);
        sourceConfig.put("database.port", "3306");
        sourceConfig.put("database.user", "root");
        sourceConfig.put("database.password", "knxy0616");
        sourceConfig.put("database.server.id", "184054");
        sourceConfig.put("database.server.name", "dbserver1");
        sourceConfig.put("database.whitelist", "inventory");
    }

    protected DebeziumMySqlByKafkaSourceTest(String sourceType) {
        super(sourceType);
    }

    @Override
    public void setServiceContainer(DebeziumMysqlContainer serviceContainer) {
        log.info("start debezium mysql server container.");
        debeziumMysqlContainer = serviceContainer;
        kafkaCluster.startService(DebeziumMysqlContainer.IMAGE_NAME, debeziumMysqlContainer);
    }

    @Override
    public void prepareSource() throws Exception {
        this.debeziumMysqlContainer.execCmd(
                "/bin/bash", "-c",
                "mysql -h 127.0.0.1 -u windwheel -p knxy0616 -e 'CREATE TABLE inventory.products'");
        log.info("debezium mysql server already contains preconfigured data.");
    }

    @Override
    public void prepareInsertEvent() throws Exception {
        this.debeziumMysqlContainer.execCmd(
                "/bin/bash", "-c",
                "mysql -h 127.0.0.1 -u windwheel -p knxy0616 -e 'SELECT * FROM inventory.products'");
        this.debeziumMysqlContainer.execCmd(
                "/bin/bash", "-c",
                "mysql -h 127.0.0.1 -u windwheel -p knxy0616 " +
                        "-e \"INSERT INTO inventory.products(name, description, weight) " +
                        "values('test-debezium', 'This is description', 2.0)\"");
    }

    @Override
    public void prepareDeleteEvent() throws Exception {
        this.debeziumMysqlContainer.execCmd(
                "/bin/bash", "-c",
                "mysql -h 127.0.0.1 -u windwheel -p knxy0616 -e 'SELECT * FROM inventory.products'");
        this.debeziumMysqlContainer.execCmd(
                "/bin/bash", "-c",
                "mysql -h 127.0.0.1 -u windwheel -p knxy0616 " +
                        "-e \"DELETE FROM inventory.products WHERE name='test-debezium'\"");
        this.debeziumMysqlContainer.execCmd(
                "/bin/bash", "-c",
                "mysql -h 127.0.0.1 -u windwheel -p knxy0616 -e 'SELECT * FROM inventory.products'");
    }

    @Override
    public void prepareUpdateEvent() throws Exception {
        this.debeziumMysqlContainer.execCmd(
                "/bin/bash", "-c",
                "mysql -h 127.0.0.1 -u windwheel -p knxy0616 " +
                        "-e \"UPDATE inventory.products set description='update description', weight=10 " +
                        "WHERE name='test-debezium'\"");
    }

    @Override
    public Map<String, String> produceSourceMessages(int numMessages) throws Exception {
        return null;
    }
}
