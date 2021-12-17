package org.apache.flink.streaming.connectors.unified.utils;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.data.RowData;

public class SourceUtils {

    public static DataStream<RowData> createSource(
            StreamExecutionEnvironment env,
            ScanTableSource.ScanRuntimeProvider changelogProvider,
            TypeInformation<RowData> output){


        DataStream<RowData> source = null;
        if (changelogProvider instanceof SourceFunctionProvider){
            SourceFunction<RowData> sourceFuntion = ((SourceFunctionProvider) changelogProvider).createSourceFunction();
            source = env.addSource(sourceFuntion);
        }else if (changelogProvider instanceof SourceProvider){
            WatermarkStrategy<RowData> result =  WatermarkStrategy.noWatermarks();
            source = env.fromSource(((SourceProvider) changelogProvider).createSource(),result,"");
        }else if (changelogProvider instanceof DataStreamScanProvider){
           source =  ((DataStreamScanProvider) changelogProvider).produceDataStream(env);
        }

        source.getTransformation().setOutputType(output);
        return source;
    }
}
