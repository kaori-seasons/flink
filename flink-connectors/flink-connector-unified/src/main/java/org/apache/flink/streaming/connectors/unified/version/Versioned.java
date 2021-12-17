package org.apache.flink.streaming.connectors.unified.version;

import java.io.Serializable;

/**
 * 设置提价的changelog版本号 这样就可以保持提交数据的顺序性，就算有等待的数据迟到
 * 由于已经打上版本号标识，所以之前产生的changelog数据是不会收到延迟数据的影响的
 * 相当于一个攒批操作
 */
public class Versioned implements Comparable<Versioned>, Serializable {


    private long generatedTs; //水印生成时间
    private long unifiedVersion; //changelog本次数据做checkpoint提交数据的版本号

    public Versioned(long generatedTs,long unifiedVersion) {
        this.generatedTs = generatedTs;
        this.unifiedVersion = unifiedVersion;
    }

    public long getGeneratedTs() {
        return generatedTs;
    }

    public void setGeneratedTs(long generatedTs) {
        this.generatedTs = generatedTs;
    }

    public long getUnifiedVersion() {
        return unifiedVersion;
    }

    public void setUnifiedVersion(long unifiedVersion) {
        this.unifiedVersion = unifiedVersion;
    }

    @Override
    public int compareTo(Versioned o) {
        return Long.compare(this.unifiedVersion,o.unifiedVersion);
    }
}
