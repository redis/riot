package com.redis.riot.core;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.util.CollectionUtils;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.ValueType;
import com.redis.spring.batch.util.PredicateItemProcessor;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;

public abstract class AbstractExport extends AbstractJobExecutable {

    private RedisReaderOptions redisReaderOptions = new RedisReaderOptions();

    protected AbstractExport(AbstractRedisClient client) {
        super(client);
    }

    public RedisReaderOptions getRedisReaderOptions() {
        return redisReaderOptions;
    }

    public void setRedisReaderOptions(RedisReaderOptions options) {
        this.redisReaderOptions = options;
    }

    protected <K, V> RedisItemReader<K, V> reader(RedisCodec<K, V> codec) {
        return reader(client, codec, redisReaderOptions);
    }

    protected <K, V> RedisItemReader<K, V> reader(AbstractRedisClient client, RedisCodec<K, V> codec,
            RedisReaderOptions options) {
        RedisItemReader<K, V> reader = new RedisItemReader<>(client, codec);
        configure(reader, options);
        reader.setKeyProcessor(keyProcessor(codec, options.getKeyFilterOptions()));
        return reader;
    }

    private <K> ItemProcessor<K, K> keyProcessor(RedisCodec<K, ?> codec, KeyFilterOptions options) {
        if (options == null || isEmpty(options)) {
            return null;
        }
        return new PredicateItemProcessor<>(options.predicate(codec));
    }

    private boolean isEmpty(KeyFilterOptions options) {
        return CollectionUtils.isEmpty(options.getSlots()) && CollectionUtils.isEmpty(options.getIncludes())
                && CollectionUtils.isEmpty(options.getExcludes());
    }

    protected abstract ValueType getValueType();

    protected void configure(RedisItemReader<?, ?> reader, RedisReaderOptions options) {
        reader.setJobRepository(jobRepository);
        reader.setValueType(getValueType());
        reader.setChunkSize(options.getChunkSize());
        reader.setDatabase(options.getDatabase());
        reader.setFlushingInterval(options.getFlushingInterval());
        reader.setIdleTimeout(options.getIdleTimeout());
        reader.setMemoryUsageLimit(options.getMemoryUsageLimit());
        reader.setMemoryUsageSamples(options.getMemoryUsageSamples());
        reader.setNotificationQueueCapacity(options.getNotificationQueueCapacity());
        reader.setOrderingStrategy(options.getOrderingStrategy());
        reader.setPollTimeout(options.getPollTimeout());
        reader.setPoolSize(options.getPoolSize());
        reader.setQueueCapacity(options.getQueueCapacity());
        reader.setReadFrom(options.getReadFrom());
        reader.setScanCount(options.getScanCount());
        reader.setScanMatch(options.getScanMatch());
        reader.setScanType(options.getScanType());
        reader.setThreads(options.getThreads());
    }

}
