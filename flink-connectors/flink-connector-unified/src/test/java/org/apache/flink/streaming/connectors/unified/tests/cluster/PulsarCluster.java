package org.apache.flink.streaming.connectors.unified.tests.cluster;

import com.google.common.collect.Maps;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.flink.streaming.connectors.unified.container.PulsarContainer;

import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

import java.util.Collections;
import java.util.Map;

import static org.apache.flink.streaming.connectors.unified.container.PulsarContainer.PULSAR_CONTAINERS_LEAVE_RUNNING;

@Slf4j
public class PulsarCluster {


    @Getter
    private final PulsarClusterSpec spec;

    @Getter
    private final String clusterName;
    private final Network network;
    private final boolean sharedCsContainer;
    private Map<String, GenericContainer<?>> externalServices = Collections.emptyMap();
    private final boolean enablePrestoWorker;


    private PulsarCluster(PulsarClusterSpec spec,Network network, boolean sharedCsContainer) {

        this.spec = spec;
        this.sharedCsContainer = sharedCsContainer;
        this.clusterName = spec.clusterName();
        this.network = network;
        this.enablePrestoWorker = spec.enablePrestoWorker();

    }



    public void startService(String networkAlias,
                             GenericContainer<?> serviceContainer) {
        log.info("Starting external service {} ...", networkAlias);
        serviceContainer.withNetwork(network);
        serviceContainer.withNetworkAliases(networkAlias);
        PulsarContainer.configureLeaveContainerRunning(serviceContainer);
        serviceContainer.start();
        log.info("Successfully start external service {}", networkAlias);
    }


    public static void stopService(String networkAlias,
                                   GenericContainer<?> serviceContainer) {
        if (PULSAR_CONTAINERS_LEAVE_RUNNING) {
            logIgnoringStopDueToLeaveRunning();
            return;
        }
        log.info("Stopping external service {} ...", networkAlias);
        serviceContainer.stop();
        log.info("Successfully stop external service {}", networkAlias);
    }

    private static void logIgnoringStopDueToLeaveRunning() {
        log.warn("Ignoring stop due to PULSAR_CONTAINERS_LEAVE_RUNNING=true.");
    }

}
