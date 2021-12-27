package org.apache.flink.streaming.connectors.unified.container;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.debezium.util.ContainerImageVersions;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.awaitility.Awaitility;

import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static java.time.temporal.ChronoUnit.SECONDS;

public class DebeziumContainer <SELF extends DebeziumContainer<SELF>> extends ChaosContainer<SELF> {

    private static final String DEBEZIUM_VERSION = ContainerImageVersions.getStableVersion("debezium/connect");

    private static final int KAFKA_CONNECT_PORT = 8083;
    private static final String TEST_PROPERTY_PREFIX = "debezium.test.";
    public static final MediaType JSON = MediaType.get("application/json; charset=utf-8");
    protected static final ObjectMapper MAPPER = new ObjectMapper();
    protected static final OkHttpClient CLIENT = new OkHttpClient();

    public DebeziumContainer(final DockerImageName containerImage,String clusterName) {
        super(containerImage, clusterName);
        defaultConfig();
    }

    public DebeziumContainer(final String containerImageName,String clusterName) {
        super(DockerImageName.parse(containerImageName), clusterName);
        defaultConfig();
    }


    private void defaultConfig() {
        setWaitStrategy(
                new LogMessageWaitStrategy()
                        .withRegEx(".*Session key updated.*")
                        .withStartupTimeout(Duration.of(60, SECONDS)));
        withEnv("GROUP_ID", "1");
        withEnv("CONFIG_STORAGE_TOPIC", "debezium_connect_config");
        withEnv("OFFSET_STORAGE_TOPIC", "debezium_connect_offsets");
        withEnv("STATUS_STORAGE_TOPIC", "debezium_connect_status");
        withEnv("CONNECT_KEY_CONVERTER_SCHEMAS_ENABLE", "false");
        withEnv("CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE", "false");
        withExposedPorts(KAFKA_CONNECT_PORT);



    }

    public  DebeziumContainer withKafka(final KafkaContainer kafkaContainer) {
        return withKafka(kafkaContainer.getNetwork(), kafkaContainer.getNetworkAliases().get(0) + ":9092");
    }

    public  DebeziumContainer withKafka(final Network network, final String bootstrapServers) {
        withNetwork(network);
        withEnv("BOOTSTRAP_SERVERS", bootstrapServers);
        return self();
    }

    @Override
    public SELF withNetwork(Network network) {
        return super.withNetwork(network);
    }

    @Override
    public SELF dependsOn(Startable... startables) {
        return super.dependsOn(startables);
    }

    @Override
    public SELF withMinimumRunningDuration(Duration minimumRunningDuration) {
        return super.withMinimumRunningDuration(minimumRunningDuration);
    }

    @Override
    public SELF withStartupAttempts(int attempts) {
        return super.withStartupAttempts(attempts);
    }

    @Override
    public SELF withStartupTimeout(Duration startupTimeout) {
        return super.withStartupTimeout(startupTimeout);
    }

    public  DebeziumContainer enableApicurioConverters() {
        withEnv("ENABLE_APICURIO_CONVERTERS", "true");
        return self();
    }

    public static int waitTimeForRecords() {
        return Integer.parseInt(System.getProperty(TEST_PROPERTY_PREFIX + "records.waittime", "2"));
    }

    public String getTarget() {
        return "http://" + getContainerIpAddress() + ":" + getMappedPort(KAFKA_CONNECT_PORT);
    }

    /**
     * Returns the "/connectors/<connector>" endpoint.
     */
    public String getConnectorsUri() {
        return getTarget() + "/connectors/";
    }

    /**
     * Returns the "/connectors/<connector>" endpoint.
     */
    public String getConnectorUri(String connectorName) {
        return getConnectorsUri() + connectorName;
    }

    /**
     * Returns the "/connectors/<connector>/pause" endpoint.
     */
    public String getPauseConnectorUri(String connectorName) {
        return getConnectorUri(connectorName) + "/pause";
    }

    /**
     * Returns the "/connectors/<connector>/pause" endpoint.
     */
    public String getResumeConnectorUri(String connectorName) {
        return getConnectorUri(connectorName) + "/resume";
    }

    /**
     * Returns the "/connectors/<connector>/status" endpoint.
     */
    public String getConnectorStatusUri(String connectorName) {
        return getConnectorUri(connectorName) + "/status";
    }

    /**
     * Returns the "/connectors/<connector>/config" endpoint.
     */
    public String getConnectorConfigUri(String connectorName) {
        return getConnectorUri(connectorName) + "/config";
    }


    private static void handleFailedResponse(Response response) {
        String responseBodyContent = "{empty response body}";
        try (final ResponseBody responseBody = response.body()) {
            if (null != responseBody) {
                responseBodyContent = responseBody.string();
            }
            throw new IllegalStateException("Unexpected response: " + response + " ; Response Body: " + responseBodyContent);
        }
        catch (IOException e) {
            throw new RuntimeException("Error connecting to Debezium container", e);
        }
    }

    private void executePOSTRequestSuccessfully(final String payload, final String fullUrl) {
        final RequestBody body = RequestBody.create(payload, JSON);
        final Request request = new Request.Builder().url(fullUrl).post(body).build();

        try (final Response response = CLIENT.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                handleFailedResponse(response);
            }
        }
        catch (IOException e) {
            throw new RuntimeException("Error connecting to Debezium container", e);
        }
    }

    private void executePUTRequestSuccessfully(final String payload, final String fullUrl) {
        final RequestBody body = RequestBody.create(payload, JSON);
        final Request request = new Request.Builder().url(fullUrl).put(body).build();

        try (final Response response = CLIENT.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                handleFailedResponse(response);
            }
        }
        catch (IOException e) {
            throw new RuntimeException("Error connecting to Debezium container", e);
        }
    }

    protected static Response executeGETRequest(Request request) {
        try {
            return CLIENT.newCall(request).execute();
        }
        catch (IOException e) {
            throw new RuntimeException("Error connecting to Debezium container", e);
        }
    }

    protected static Response executeGETRequestSuccessfully(Request request) {
        final Response response = executeGETRequest(request);
        if (!response.isSuccessful()) {
            handleFailedResponse(response);
        }
        return response;
    }

    public boolean connectorIsNotRegistered(String connectorName) {
        final Request request = new Request.Builder().url(getConnectorUri(connectorName)).build();
        try (final Response response = executeGETRequest(request)) {
            return response.code() == 404;
        }
    }

    protected void deleteDebeziumConnector(String connectorName) {
        final Request request = new Request.Builder().url(getConnectorUri(connectorName)).delete().build();
        executeGETRequestSuccessfully(request).close();
    }

    public void deleteConnector(String connectorName) {
        deleteDebeziumConnector(connectorName);

        Awaitility.await()
                .atMost(waitTimeForRecords() * 5L, TimeUnit.SECONDS)
                .until(() -> connectorIsNotRegistered(connectorName));
    }

    public List<String> getRegisteredConnectors() {
        final Request request = new Request.Builder().url(getConnectorsUri()).build();
        try (final ResponseBody responseBody = executeGETRequestSuccessfully(request).body()) {
            if (null != responseBody) {
                return MAPPER.readValue(responseBody.string(), new TypeReference<List<String>>() {
                });
            }
        }
        catch (IOException e) {
            throw new IllegalStateException("Error fetching list of registered connectors", e);
        }
        return Collections.emptyList();
    }

    public boolean isConnectorConfigured(String connectorName) {
        final Request request = new Request.Builder().url(getConnectorUri(connectorName)).build();
        try (final Response response = executeGETRequest(request)) {
            return response.isSuccessful();
        }
    }

    public void ensureConnectorRegistered(String connectorName) {
        Awaitility.await()
                .atMost(waitTimeForRecords() * 5L, TimeUnit.SECONDS)
                .until(() -> isConnectorConfigured(connectorName));
    }

    public void deleteAllConnectors() {
        final List<String> connectorNames = getRegisteredConnectors();

        for (String connectorName : connectorNames) {
            deleteDebeziumConnector(connectorName);
        }

        Awaitility.await()
                .atMost(waitTimeForRecords() * 5L, TimeUnit.SECONDS)
                .until(() -> getRegisteredConnectors().size() == 0);
    }


    public String getConnectorConfigProperty(String connectorName, String configPropertyName) {
        final Request request = new Request.Builder().url(getConnectorConfigUri(connectorName)).get().build();

        try (final ResponseBody responseBody = executeGETRequestSuccessfully(request).body()) {
            if (null != responseBody) {
                final ObjectNode parsedObject = (ObjectNode) MAPPER.readTree(responseBody.string());
                return parsedObject.get(configPropertyName).asText();
            }
            return null;
        }
        catch (IOException e) {
            throw new IllegalStateException("Error fetching connector config property for connector: " + connectorName, e);
        }
    }


}
