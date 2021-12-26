package org.apache.flink.streaming.connectors.unified.tests;


import lombok.Getter;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.common.schema.KeyValue;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.shaded.com.google.common.collect.Maps;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;


/**
 * 这里会定义一些changelog行为 insert update delete
 */
@Getter
public abstract class SourceTester<ServiceContainerT extends GenericContainer> {


    private Logger log = LoggerFactory.getLogger(SourceTester.class);

    public static final String INSERT = "INSERT";

    public static final String DELETE = "DELETE";

    public static final String UPDATE = "UPDATE";

    protected final String sourceType;
    protected final Map<String, Object> sourceConfig;

    protected int numEntriesToInsert = 1;
    protected int numEntriesExpectAfterStart = 9;

    //DEBEZIUM解析出来的binlog格式
    public static final Set<String> DEBEZIUM_FIELD_SET = new HashSet<String>() {{
        add("before");
        add("after");
        add("source");
        add("op");
        add("ts_ms");
        add("transaction");
    }};

    protected SourceTester(String sourceType) {
        this.sourceType = sourceType;
        this.sourceConfig = Maps.newHashMap();
    }

    public abstract void setServiceContainer(ServiceContainerT serviceContainer);

    public String sourceType() {
        return sourceType;
    }

    public Map<String, Object> sourceConfig() {
        return sourceConfig;
    }

    public abstract void prepareSource() throws Exception;

    public abstract void prepareInsertEvent() throws Exception;

    public abstract void prepareDeleteEvent() throws Exception;

    public abstract void prepareUpdateEvent() throws Exception;

    public abstract Map<String, String> produceSourceMessages(int numMessages) throws Exception;

    /**
     * 这里只验证json格式的changelog
     * @param consumer
     * @param number
     * @param eventType
     * @param converterClassName
     * @throws Exception
     */
    public void validateSourceResult( int number,
            String eventType, String converterClassName) throws Exception {
        doPreValidationCheck(eventType);

        doPostValidationCheck(eventType);
    }

    /**
     * Execute before regular validation to check database-specific state.
     */
    public void doPreValidationCheck(String eventType) {
        log.info("pre-validation of {}", eventType);
    }

    /**
     * Execute after regular validation to check database-specific state.
     */
    public void doPostValidationCheck(String eventType) {
        log.info("post-validation of {}", eventType);
    }

    public void validateSourceResultJson(Consumer<KeyValue<byte[], byte[]>> consumer, int number, String eventType) throws Exception {
        int recordsNumber = 0;
        Message<KeyValue<byte[], byte[]>> msg = consumer.receive(initialDelayForMsgReceive(), TimeUnit.SECONDS);
        while(msg != null) {
            recordsNumber ++;
            final String key = new String(msg.getValue().getKey());
            final String value = new String(msg.getValue().getValue());
            log.info("Received message: key = {}, value = {}.", key, value);
            Assert.assertTrue(key.contains(this.keyContains()));
            Assert.assertTrue(value.contains(this.valueContains()));
            if (eventType != null) {
                Assert.assertTrue(value.contains(this.eventContains(eventType, true)));
            }
        }

        Assert.assertEquals(recordsNumber, number);
        log.info("Stop {} server container. topic: {} has {} records.", getSourceType(), consumer.getTopic(), recordsNumber);
    }


    public int initialDelayForMsgReceive(){
        return 2;
    }

    public String keyContains() {
        return "dbserver1.inventory.products.Key";
    }

    public String valueContains() {
        return "dbserver1.inventory.products.Value";
    }

    public String eventContains(String eventType, boolean isJson) {
        if (eventType.equals(INSERT)) {
            return isJson ? "\"op\":\"c\"" : "c";
        } else if (eventType.equals(UPDATE)) {
            return isJson ? "\"op\":\"u\"" : "u";
        } else {
            return isJson ? "\"op\":\"d\"" : "d";
        }
    }

}



