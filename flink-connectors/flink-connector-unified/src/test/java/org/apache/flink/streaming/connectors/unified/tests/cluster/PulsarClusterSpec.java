package org.apache.flink.streaming.connectors.unified.tests.cluster;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.Singular;
import lombok.experimental.Accessors;

import org.apache.flink.streaming.connectors.unified.container.PulsarContainer;

import org.apache.pulsar.common.protocol.Commands;
import org.testcontainers.containers.GenericContainer;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

/**
 * 描述一个pulsar集群的信息
 */

@Builder
@Accessors(fluent = true)
@Getter
@Setter
public class PulsarClusterSpec {

    /**
     * pulsar的集群名
     */
    String clusterName;

    /**
     * bookie的数量
     */
    @Builder.Default
    int numsBookies = 2;

    /**
     * broker的数量
     */
    @Builder.Default
    int numsBrokers = 2;


    /**
     * pulsar dns代理服务器的数量
     */
    @Builder.Default
    int numsProxies = 1;

    /**
     * function计算模块的worker数量
     */
    @Builder.Default
    int numFunctionWorkers = 0;

    /**
     * 启用presto worker节点
     */
    @Builder.Default
    boolean enablePrestoWorker = false;

    /**
     * 允许查询最后一条消息
     */
    @Builder.Default
    boolean queryLastMessage = false;


    /**
     * worker正在处理中或者 等待分配worker
     */
    @Builder.Default
    FunctionRuntimeType functionRuntimeType = FunctionRuntimeType.PROCESS;

    /**
     * 返回一个需要使用pulsar资源的容器列表
     */
    @Singular
    Map<String, GenericContainer<?>> externalServices = Collections.EMPTY_MAP;

    @Builder.Default
    boolean enableContainerLog = false;

    /**
     * Provide a map of paths (in the classpath) to mount as volumes inside the containers
     */
    @Builder.Default
    Map<String, String> classPathVolumeMounts = new TreeMap<>();

    /**
     * Pulsar Test Image Name
     *
     * @return the version of the pulsar test image to use
     */
    @Builder.Default
    String pulsarTestImage = PulsarContainer.DEFAULT_IMAGE_NAME;

    /**
     * Specify envs for proxy.
     */
    Map<String, String> proxyEnvs;

    /**
     * Specify envs for broker.
     */
    Map<String, String> brokerEnvs;

    /**
     * Specify mount files.
     */
    Map<String, String> proxyMountFiles;

    /**
     * Specify mount files.
     */
    Map<String, String> brokerMountFiles;

    @Builder.Default
    int maxMessageSize = Commands.DEFAULT_MAX_MESSAGE_SIZE;


}
