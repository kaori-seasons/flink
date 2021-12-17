package org.apache.flink.streaming.connectors.unified.integrate;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.connectors.unified.version.Versioned;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.ProjectionCodeGenerator;
import org.apache.flink.table.runtime.generated.Projection;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 批流两端等待先到达的数据对齐的实现
 */
public class DataIntegrateKeyedCoProcessFunction extends KeyedCoProcessFunction<RowData,RowData,RowData,RowData> {


    private Long fixedDelay; //水印的延迟时间
    private TypeSerializer<Versioned> versionedTypeSerializer; //从format当中提取的changelog提交版本号
    private RowType changelogInputRowType; //changelog的输入schema格式
    private RowType outputRowType; //批处理的输入schema
    private TypeInformation<RowData> outputTypeInformation;  //根据不通的format进行适配
    private TypeInformation<Versioned> versionedTypeInformation;
    private Boolean sendDataBehindWatermark;

    private ValueState<Boolean> bulkDataProcessed; //因为批处理这边 需要对齐已经到达的数据 而在双流join的场景下
    // process函数调用的state全局变量 总是会存在一端先到，一端后到的情况 ,所以这其实就是标记下 哪部分数据达到了 可以和bulk源做join了
    private ValueState<RowData> bulkData; //已经达到的bulk数据
    private ValueState<Versioned> changelogVersion; //changelog本次到达 准备基于定时器提交的版本号,
    // 这里就是上一个function中通过特定的format提取出来的版本号，咋说呢 有点像MVCC吧
    private ValueState<Long> registerTime;
    private MapState<Long, List<RowData>> registeredChangelogData;
    private Projection copyRowProjection; //投影？他是想做列剪裁么？

    public DataIntegrateKeyedCoProcessFunction(
            Long fixedDelay,
            TypeSerializer<Versioned> versionedTypeSerializer,
            RowType changelogInputRowType,
            RowType outputRowType,
            TypeInformation<RowData> outputTypeInformation,
            TypeInformation<Versioned> versionedTypeInformation,
            Boolean sendDataBehindWatermark) {
        this.fixedDelay = fixedDelay;
        this.versionedTypeSerializer = versionedTypeSerializer;
        this.changelogInputRowType = changelogInputRowType;
        this.outputRowType = outputRowType;
        this.outputTypeInformation = outputTypeInformation;
        this.versionedTypeInformation = versionedTypeInformation;
        this.sendDataBehindWatermark = sendDataBehindWatermark;
    }

    @Override
    public void open(Configuration parameters) throws Exception {

        bulkDataProcessed = getRuntimeContext().getState(
                new ValueStateDescriptor<Boolean>("b-d-p", Types.BOOLEAN));
        bulkData = getRuntimeContext().getState(
                new ValueStateDescriptor("b-d", outputTypeInformation)
        );
        changelogVersion = getRuntimeContext().getState(
                new ValueStateDescriptor<Versioned>("c-l-v", versionedTypeInformation));
        registerTime = getRuntimeContext().getState(
                new ValueStateDescriptor<Long>("r-t", Types.LONG));

        registeredChangelogData = getRuntimeContext().getMapState(
                new MapStateDescriptor<Long,List<RowData>>(
                "r-c-d",
                Types.LONG,
                Types.LIST(outputTypeInformation)));



        final int limit = outputRowType.getFieldCount();
        int[] generateNumber = new int[limit];
        AtomicInteger count = new AtomicInteger(-1);
        for(int i=0;i<limit;i++){
            if (count.get() < limit){
                int needCount = count.updateAndGet((x) -> x+1);
                generateNumber[i] = needCount;
            }
        }

        copyRowProjection =ProjectionCodeGenerator.generateProjection(
                CodeGeneratorContext.apply(new TableConfig()),
                        "CopyRowProjection",
                        changelogInputRowType,
                        outputRowType,
                generateNumber)
                .newInstance(Thread.currentThread().getContextClassLoader());

        super.open(parameters);
    }



    @Override
    public void processElement1(
            RowData value,
            KeyedCoProcessFunction<RowData, RowData, RowData, RowData>.Context ctx,
            Collector<RowData> out) throws Exception {

        //TODO 如果当前changelog流还未有数据达到，并且没有正在处理中同一个批次的bulkData
        // 以及此时没有到触发watermark的等待时间
        if (getLastChangelogVersion() == null) {
            long curProcessingTime = ctx.timerService().currentProcessingTime();
            long triggerTime = curProcessingTime + fixedDelay;
            ctx.timerService().registerEventTimeTimer(triggerTime);
            bulkData.update(value);
            registerTime.update(triggerTime);
        }
    }

    @Override
    public void processElement2(
            RowData value,
            KeyedCoProcessFunction<RowData, RowData, RowData, RowData>.Context ctx,
            Collector<RowData> out) throws Exception {

    }

    @Override
    public void onTimer(
            long timestamp,
            KeyedCoProcessFunction<RowData, RowData, RowData, RowData>.OnTimerContext ctx,
            Collector<RowData> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
    }


    //取到changelog本次提交数据的版本信息
    private Versioned getLastChangelogVersion() throws IOException {
        ValueState<Versioned> versionedValueState = this.changelogVersion;
        return versionedValueState.value();
    }
}
