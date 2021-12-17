package org.apache.flink.streaming.connectors.unified.serializate;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.connectors.unified.version.Versioned;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RawType;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 将changelog当中的版本号提取出来 单独作为一个字段
 * 并且解析其他字段
 */
public interface VersionedDeserializationSchema extends DeserializationSchema<RowData> {


    RowType getRowDataType();

    default TypeSerializer<Versioned> getVersionTypeSerializer(){
        return TypeInformation.of(Versioned.class).createSerializer(new ExecutionConfig());
    }

    @Override
    default RowData deserialize(byte[] message) throws IOException {
        throw  new UnsupportedOperationException();
    }

    @Override
    default boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    @Override
    default TypeInformation getProducedType() {
        return InternalTypeInfo.of(getActualRowType());
    }


    default RowType getActualRowType(){
        List<LogicalType> types = new ArrayList<>();
        types = getRowDataType().getFields().stream().map(RowType.RowField::getType)
                        .collect(Collectors.toList());

        types.add(new RawType(Versioned.class,getVersionTypeSerializer()));
        return RowType.of(types.toArray(new LogicalType[0]));
    }
}
