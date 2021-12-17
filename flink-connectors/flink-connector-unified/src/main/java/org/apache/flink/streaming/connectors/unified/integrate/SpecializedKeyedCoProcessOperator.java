package org.apache.flink.streaming.connectors.unified.integrate;

import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.lang.reflect.Method;

public class SpecializedKeyedCoProcessOperator<K,IN1,IN2,OUT> extends KeyedCoProcessOperator<K,IN1,IN2,OUT> {

    public SpecializedKeyedCoProcessOperator(KeyedCoProcessFunction<K, IN1, IN2, OUT> keyedCoProcessFunction) {
        super(keyedCoProcessFunction);
    }

    @Override
    public void open() throws Exception {
        Class<?> mainClass =  Class.forName(AbstractStreamOperator.class.getName());
        Method method =  mainClass.getDeclaredMethod("processWatermark1", Watermark.class);
        method.invoke(null,Long.MAX_VALUE);
    }
}
