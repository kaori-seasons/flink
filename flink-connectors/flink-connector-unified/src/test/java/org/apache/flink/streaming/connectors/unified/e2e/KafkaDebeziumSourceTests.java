package org.apache.flink.streaming.connectors.unified.e2e;

import lombok.extern.slf4j.Slf4j;

import org.apache.flink.streaming.connectors.unified.container.DebeziumMysqlContainer;
import org.apache.flink.streaming.connectors.unified.container.KafkaContainer;

import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;

@Slf4j
public class KafkaDebeziumSourceTests {



    private String clusterName;

    @Before
    public void setupCluster(){
        clusterName = Stream.of(this.getClass().getSimpleName(), randomName(5))
                .filter(s -> s != null && !s.isEmpty())
                .collect(joining("-"));
    }

    @Test
    private void testDebeziumMySqlConnect(String converterClassName, boolean jsonWithEnvelope) throws Exception {



        //setup kafka server
        KafkaContainer kafkaContainer = new KafkaContainer();
        kafkaContainer.start();

        DebeziumMySqlByKafkaSourceTest sourceTester = new DebeziumMySqlByKafkaSourceTest(kafkaContainer, converterClassName);
        sourceTester.getSourceConfig().put("json-with-envelope", jsonWithEnvelope);

        // setup debezium mysql server
        DebeziumMysqlContainer mySQLContainer = new DebeziumMysqlContainer(clusterName);
        sourceTester.setServiceContainer(mySQLContainer);



        //FIXME 从Debezium解析的changelog操作 导入到kafka
        // 替换成flink cdc



    }

    public static String randomName(int numChars) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < numChars; i++) {
            sb.append((char) (ThreadLocalRandom.current().nextInt(26) + 'a'));
        }
        return sb.toString();
    }


}
