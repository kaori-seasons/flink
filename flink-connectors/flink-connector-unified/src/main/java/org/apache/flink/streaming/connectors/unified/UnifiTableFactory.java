package org.apache.flink.streaming.connectors.unified;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DeserializationSchemaFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.TableFactory;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 需要指定的Table SPI
 */
public abstract class UnifiTableFactory implements DynamicTableSourceFactory {


    public  static final String CHANGELOG_PREFIX = "_changelog.";
    public static final String BULK_PREFIX = "_bulk.";



    //连接器选项
    ConfigOption<Long> FXIED_DELAY = org.apache.flink.configuration.ConfigOptions
            .key("fixed-delay")
            .longType()
            .defaultValue(10 * 1000L)
            .withDescription("");

    ConfigOption<Integer> CHANGELOG_PARAL = org.apache.flink.configuration.ConfigOptions
                    .key("changelog-parallelism")
                    .intType()
                    .defaultValue(1)
                    .withDescription("");


    ConfigOption<Integer> BULK_PARAL = org.apache.flink.configuration.ConfigOptions
            .key("bulk-parallelism")
            .intType()
            .noDefaultValue()
            .withDescription("");

    //TODO detected //代理
    // 背景其实是这样的 因为我们开发的这个connector，
    // 必然是期望跟sql平台可以集成的
    // 那对于上层的批 或者流用的format 需要强制限定为一致，
    // 这样子对于同一种schema才能
    // 进行join操作，所以这个detected的方法的作用
    // 我猜测是用来代理两端数据统一的format
    // DeserializationFormatFactory
    public abstract void detected(DecodingFormat tableFactory);


    private class InternalContext implements DynamicTableFactory.Context {

        private ResolvedCatalogTable catalogTable;
        private Context context;

        public InternalContext(ResolvedCatalogTable catalogTable,Context context) {
           this.catalogTable = catalogTable;
           this.context = context;
        }

        @Override
        public ObjectIdentifier getObjectIdentifier() {
            return context.getObjectIdentifier();
        }

        @Override
        public ResolvedCatalogTable getCatalogTable() {
            return catalogTable;
        }

        @Override
        public ReadableConfig getConfiguration() {
            return context.getConfiguration();
        }

        @Override
        public ClassLoader getClassLoader() {
            return context.getClassLoader();
        }

        @Override
        public boolean isTemporary() {
            return context.isTemporary();
        }
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {

        ResolvedCatalogTable catalogTable =  context.getCatalogTable();
        Map<String,String> options =  catalogTable.getOptions();

        Map<String,String> bulkOptions = getBulkChangeOptions(options);
        Map<String,String> realtimeOptions = getRealtimeChangeOptions(options);

        //组装bulk一端的数据格式化信息
        ResolvedCatalogTable bulkCatalogTable =  catalogTable.copy(bulkOptions);
        ScanTableSource bulkTableSource =  (ScanTableSource)FactoryUtil.createTableSource(null,context.getObjectIdentifier(),
                bulkCatalogTable,context.getConfiguration(),context.getClassLoader(),
                context.isTemporary());

        //组装changelog一端的数据格式化信息
        ResolvedCatalogTable changelogTable = catalogTable.copy(realtimeOptions);
        ScanTableSource changelogTableSource =  (ScanTableSource)FactoryUtil.createTableSource(null,context.getObjectIdentifier(),changelogTable,
                context.getConfiguration(),context.getClassLoader(), context.isTemporary());




        //这里根据需要加载对应的DynamicTableFactory对应的工厂
        FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this,new InternalContext(changelogTable,context));
        DecodingFormat decodingFormat = helper.discoverDecodingFormat(DeserializationFormatFactory.class,FactoryUtil.FORMAT);

        long fixedDelay = helper.getOptions().get(FXIED_DELAY);
        int bulkParallelism = helper.getOptions().getOptional(BULK_PARAL).orElse(null).intValue();

        int changelogParallelism = helper.getOptions().getOptional(CHANGELOG_PARAL).orElse(null).intValue();
        //TODO 底层需要根据不同的format进行适配
        detected(decodingFormat);


        return new UnifiedTableSource(bulkTableSource,changelogTableSource,catalogTable.getSchema(),decodingFormat,fixedDelay,bulkParallelism,changelogParallelism);
    }

    /**
     * 获取实时数据源的链接信息
     */

    public Map<String,String> getRealtimeChangeOptions(Map<String,String> options){
        return  getOptions(CHANGELOG_PREFIX,options);
    }

    /**
     * 获取批量数据源的链接信息
     */
    public Map<String,String> getBulkChangeOptions(Map<String,String> options){
        return getOptions(BULK_PREFIX,options);
    }

    /**
     * 取连接器的配置信息
     * @param prefix
     * @param options
     * @return
     */
    public Map<String,String> getOptions(String prefix,Map<String,String> options){

        options = options.entrySet().stream().filter( (in) ->{
            String key = in.getKey();
           if (key.startsWith(prefix)){
               return true;
           }
            return false;
        } ).map( (in) -> {
            String key = in.getKey();
            return key.substring(prefix.length());
        }).collect(Collectors.toMap(String::new,String::new));

        return options;
    }

    @Override
    public String factoryIdentifier() {
        return null;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return null;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return null;
    }
}
