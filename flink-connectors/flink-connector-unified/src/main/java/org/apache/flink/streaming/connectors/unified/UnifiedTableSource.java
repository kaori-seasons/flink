package org.apache.flink.streaming.connectors.unified;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.connectors.unified.integrate.DataIntegrateKeyedCoProcessFunction;
import org.apache.flink.streaming.connectors.unified.integrate.SpecializedKeyedCoProcessOperator;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.unified.serializate.VersionedDeserializationSchema;
import org.apache.flink.streaming.connectors.unified.utils.SourceUtils;
import org.apache.flink.streaming.connectors.unified.version.Versioned;
import org.apache.flink.streaming.util.keys.KeySelectorUtil;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.RowKind;

/**
 * 对于实时的数据 新到达的数据会以changelog的形式
 * 存储到kafka/pulsar上
 * 所以这里实时数据源其实是在消费changelog的数据
 */
public class UnifiedTableSource implements ScanTableSource {


    private ScanTableSource bulkSource;

    private ScanTableSource realtimeSource;

    private DecodingFormat<VersionedDeserializationSchema> decodingFormat;

    private TableSchema tableSchema;

    private Long fixedDelay;

    private Integer bulkParallelism;

    private Integer changelogParallelism;


    public UnifiedTableSource(
            ScanTableSource bulkSource,
            ScanTableSource realtimeSource,
            TableSchema tableSchema,
            DecodingFormat<VersionedDeserializationSchema> decodingFormat,
            Long fixedDelay,
            Integer bulkParallelism,
            Integer changelogParallelism) {
        this.bulkSource = bulkSource;
        this.realtimeSource = realtimeSource;
        this.tableSchema = tableSchema;
        this.fixedDelay = fixedDelay;
        this.bulkParallelism = bulkParallelism;
        this.changelogParallelism = changelogParallelism;
        this.decodingFormat = decodingFormat;
    }

    /**
     * 对于批流融合的连接器 如果要实现接入，那么需要
     * 1.暴露批和流的RuntimeProvider层
     * 2.要从批和流各自的RowData格式 解析出具体的类型RowType(即schema)
     */
    private class UnifiedTableDataStreamScamProvider implements DataStreamScanProvider {

        private ScanRuntimeProvider realtimeScanRuntimeProvider;
        private ScanRuntimeProvider bulkScanRuntimeProvider;
        private Long fixedDelay;
        private TypeSerializer<Versioned> typeSerializer;
        private RowType realtimeChangelogRowType;
        private RowType bulkChangelogRowType;
        private TypeInformation<RowData> outputTypeInfoformation;


        public UnifiedTableDataStreamScamProvider(
                ScanRuntimeProvider bulkScanRuntimeProvider,
                ScanRuntimeProvider realtimeScanRuntimeProvider,
                Long fixedDelay,
                TypeSerializer<Versioned> typeSerializer,
                RowType realtimeChangelogRowType,
                RowType bulkChangelogRowType,
                TypeInformation<RowData> outputTypeInfoformation) {
            this.bulkScanRuntimeProvider = bulkScanRuntimeProvider;
            this.realtimeScanRuntimeProvider = realtimeScanRuntimeProvider;
            this.fixedDelay = fixedDelay;
            this.typeSerializer = typeSerializer;
            this.realtimeChangelogRowType = realtimeChangelogRowType;
            this.bulkChangelogRowType = bulkChangelogRowType;
            this.outputTypeInfoformation = outputTypeInfoformation;
        }

        @Override
        public DataStream<RowData> produceDataStream(StreamExecutionEnvironment execEnv) {



            //如果传入的数据存在嵌套数据结构的情况 可以先将RowType转换为Java类型
            DataStream<RowData> bulkSource = SourceUtils.createSource(execEnv,bulkScanRuntimeProvider,outputTypeInfoformation);
            DataStream<RowData> streamSource = SourceUtils.createSource(execEnv,realtimeScanRuntimeProvider,InternalTypeInfo.of(realtimeChangelogRowType));

            //FIXME 解析批和流的schema
            int[] tablePrimaryColumns =  TableSchemaUtils.getPrimaryKeyIndices(tableSchema);
            KeySelectorUtil.ArrayKeySelector<RowData> bulkKeySelector =  KeySelectorUtil.getSelectorForArray(tablePrimaryColumns,bulkSource.getTransformation().getOutputType());

            KeySelectorUtil.ArrayKeySelector<RowData> realtimeKeySelector =  KeySelectorUtil.getSelectorForArray(tablePrimaryColumns,streamSource.getTransformation().getOutputType());

            WatermarkGeneratorSupplier<RowData> timestampsAndWatermarksContext =  new WatermarkGeneratorSupplier<RowData>(){

                TypeSerializer<Versioned> currentType =  typeSerializer;
                @Override
                public WatermarkGenerator<RowData> createWatermarkGenerator(Context context) {
                    
                    //定义当前数据达到时间，并从实时数据流中更新changelog最新达到的时间
                    Long currTs = Long.MIN_VALUE;
                    
                    return new WatermarkGenerator<RowData>() {
                        @Override
                        public void onEvent(
                                RowData event,
                                long eventTimestamp,
                                WatermarkOutput output) {

                            //一行数据中字段的数量
                            RawValueData<Versioned> versionedRawValueData = event.getRawValue(event.getArity()-1);
                            long generateTs = versionedRawValueData.toObject(currentType).getGeneratedTs();

                            if (generateTs < eventTimestamp){
                                generateTs = eventTimestamp;
                                //触发水位线
                                output.emitWatermark(new Watermark(generateTs));
                            }
                        }

                        @Override
                        public void onPeriodicEmit(WatermarkOutput output) {
                            output.emitWatermark(new Watermark(currTs));
                        }
                    };


                }
            };

            streamSource.getTransformation().setParallelism(changelogParallelism);
            bulkSource.getTransformation().setParallelism(bulkParallelism);


            //开启递送数据的水印
            SingleOutputStreamOperator out =  bulkSource.connect(streamSource.assignTimestampsAndWatermarks(WatermarkStrategy.forGenerator(timestampsAndWatermarksContext)))
                    .keyBy(bulkKeySelector,realtimeKeySelector)
                    .transform("dataInegrate",
                            outputTypeInfoformation,
                            new SpecializedKeyedCoProcessOperator(new DataIntegrateKeyedCoProcessFunction(
                                    fixedDelay,typeSerializer,realtimeChangelogRowType,bulkChangelogRowType,
                                    outputTypeInfoformation,TypeInformation.of(Versioned.class),true
                            )));
            out.setParallelism(execEnv.getParallelism());



            return out;
        }

        @Override
        public boolean isBounded() {
            return false;
        }
    }


    @Override
    public DynamicTableSource copy() {
        return null;
    }

    @Override
    public String asSummaryString() {
        return null;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode
                .newBuilder()
                .addContainedKind(RowKind.DELETE)
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .addContainedKind(RowKind.UPDATE_BEFORE)
                .build();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        DataType dataType =  tableSchema.toPhysicalRowDataType();
        VersionedDeserializationSchema versionedDeserializationSchema =  decodingFormat.createRuntimeDecoder(scanContext,dataType);
        TypeSerializer<Versioned> typeSerializer =  versionedDeserializationSchema.getVersionTypeSerializer();

        RowType changelogOutPutType = versionedDeserializationSchema.getActualRowType();
        RowType  outputRowType =  (RowType)dataType.getLogicalType();

        return new UnifiedTableDataStreamScamProvider(bulkSource.getScanRuntimeProvider(scanContext),
                realtimeSource.getScanRuntimeProvider(scanContext),
                fixedDelay,typeSerializer,changelogOutPutType,outputRowType,InternalTypeInfo.of(outputRowType));
    }
}
