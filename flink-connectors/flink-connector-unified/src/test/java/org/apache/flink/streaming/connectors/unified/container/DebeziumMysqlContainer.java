package org.apache.flink.streaming.connectors.unified.container;

import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;

public class DebeziumMysqlContainer extends ChaosContainer<DebeziumMysqlContainer>{


    public static final String NAME = "debezium-mysql-example";
    static final Integer[] PORTS = { 3306 };

    public static final String IMAGE_NAME = "debezium/example-mysql:1.8.0.Final";

    public DebeziumMysqlContainer(String clusterName) {
        super(clusterName, IMAGE_NAME);
        this.withEnv("MYSQL_USER", "windwheel");
        this.withEnv("MYSQL_PASSWORD", "knxy0616");
        this.withEnv("MYSQL_ROOT_PASSWORD", "knxy0616");
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
