package org.apache.flink.streaming.connectors.unified.factory;

import org.apache.flink.streaming.connectors.unified.UnifiTableFactory;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.TableFactory;

import java.util.Map;

/**
 * 需要实现一种流and批融合的SPI，交给用户去实现
 */
public class HiveAndKafkaFactory extends UnifiTableFactory {


    //TODO 根据不同的序列化器进行format的调用
    @Override
    public void detected(DecodingFormat tableFactory) {

    }
}
