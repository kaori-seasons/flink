package org.apache.flink.streaming.connectors.unified;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.flink.streaming.connectors.unified.container.DebeziumMysqlContainer;
import org.apache.flink.streaming.connectors.unified.container.PulsarContainer;
import org.apache.flink.streaming.connectors.unified.tests.SourceTester;

import org.apache.flink.streaming.connectors.unified.tests.cluster.PulsarCluster;

import org.testcontainers.containers.GenericContainer;

import java.util.Map;


/**
 * pulsar的单元测试需要依赖启动脚本 仅限于需要对topic进行操作的时候
 */
@Slf4j
public class DebeziumMySqlSourceTest extends SourceTester<DebeziumMysqlContainer> {

    private static final String NAME = "debezium-mysql";

    private final String pulsarServiceUrl;

    @Getter
    private DebeziumMysqlContainer debeziumMysqlContainer;

    private final PulsarCluster pulsarCluster;




    protected DebeziumMySqlSourceTest(PulsarCluster cluster,String converterClassName) {
        super(NAME);
        this.pulsarCluster = cluster;
        pulsarServiceUrl = "pulsar://pulsar-proxy:" + PulsarContainer.BROKER_PORT;

        sourceConfig.put("database.hostname", DebeziumMysqlContainer.NAME);
        sourceConfig.put("database.port", "3306");
        sourceConfig.put("database.user", "root");
        sourceConfig.put("database.password", "knxy0616");
        sourceConfig.put("database.server.id", "184054");
        sourceConfig.put("database.server.name", "dbserver1");
        sourceConfig.put("database.whitelist", "inventory");
        sourceConfig.put("database.history.pulsar.service.url", pulsarServiceUrl);
        sourceConfig.put("key.converter", converterClassName);
        sourceConfig.put("value.converter", converterClassName);
        sourceConfig.put("topic.namespace", "debezium/mysql-" +
                (converterClassName.endsWith("AvroConverter") ? "avro" : "json"));

    }

    @Override
    public void setServiceContainer(DebeziumMysqlContainer serviceContainer) {
        log.info("start debezium mysql server container.");
        debeziumMysqlContainer = serviceContainer;
        pulsarCluster.startService(DebeziumMysqlContainer.NAME, debeziumMysqlContainer);
    }

    @Override
    public void prepareSource() throws Exception {
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
                "mysql -h 127.0.0.1 -u windwheel -p windwheel -e 'SELECT * FROM inventory.products'");
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
