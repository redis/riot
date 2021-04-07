package com.redislabs.riot;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.redis.support.RedisClusterScanSizeEstimator;
import org.springframework.batch.item.redis.support.RedisScanSizeEstimator;
import org.springframework.batch.item.redis.support.ScanSizeEstimator;
import picocli.CommandLine;

import java.util.function.Supplier;

@Slf4j
public abstract class AbstractTransferCommand extends AbstractTaskCommand {

    @CommandLine.Mixin
    private TransferOptions transferOptions = TransferOptions.builder().build();

    protected <I,O> StepBuilder<I, O> stepBuilder(String name, String taskName) {
        return new StepBuilder<I, O>(jobFactory, transferOptions).name(name).taskName(taskName).initialMax(initialMax());
    }

    protected Supplier<Long> initialMax() {
        return null;
    }

    @SuppressWarnings("unchecked")
    protected Supplier<Long> initialMax(ScanSizeEstimator.Options options) {
        ScanSizeEstimator<?> estimator = isCluster() ? new RedisClusterScanSizeEstimator((GenericObjectPool<StatefulRedisClusterConnection<String, String>>) pool) : new RedisScanSizeEstimator((GenericObjectPool<StatefulRedisConnection<String, String>>) pool);
        return () -> {
            try {
                return estimator.estimate(options);
            } catch (Exception e) {
                log.warn("Could not estimate scan size", e);
                return null;
            }
        };
    }

}
