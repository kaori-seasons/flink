package org.apache.flink.streaming.connectors.unified.container;

import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;

public class DebeziumMysqlContainer extends ChaosContainer<DebeziumMysqlContainer>{


    public static final String NAME = "debezium-mysql-example";
    static final Integer[] PORTS = { 3306 };

    private static final String IMAGE_NAME = "debezium/example-mysql:0.8";

    protected DebeziumMysqlContainer(String clusterName, String image) {
        super(clusterName, IMAGE_NAME);
        this.withEnv("MYSQL_USER", "root");
        this.withEnv("MYSQL_PASSWORD", "123456");
        this.withEnv("MYSQL_ROOT_PASSWORD", "123456");
    }


    @Override
    public String getContainerName() {
        return clusterName;
    }

    @Override
    protected void configure() {
        super.configure();
        this.withNetworkAliases(NAME)
                .withExposedPorts(PORTS)
                .withCreateContainerCmdModifier(createContainerCmd -> {
                    createContainerCmd.withHostName(NAME);
                    createContainerCmd.withName(getContainerName());
                })
                .waitingFor(new HostPortWaitStrategy());
    }




}
