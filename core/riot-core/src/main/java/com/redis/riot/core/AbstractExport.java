package com.redis.riot.core;

import java.util.function.Function;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.function.FunctionItemProcessor;
import org.springframework.util.CollectionUtils;

import com.redis.spring.batch.KeyValue;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.ValueType;
import com.redis.spring.batch.util.PredicateItemProcessor;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public abstract class AbstractExport<K, V> extends AbstractJobExecutable {

    protected final RedisCodec<K, V> codec;

    protected RedisReaderOptions readerOptions = new RedisReaderOptions();

    protected KeyValueProcessorOptions processorOptions = new KeyValueProcessorOptions();

    protected AbstractExport(AbstractRedisClient client, RedisCodec<K, V> codec) {
        super(client);
        this.codec = codec;
    }

    public void setProcessorOptions(KeyValueProcessorOptions options) {
        this.processorOptions = options;
    }

    public void setReaderOptions(RedisReaderOptions options) {
        this.readerOptions = options;
    }

    protected ItemProcessor<KeyValue<K>, KeyValue<K>> keyValueProcessor() {
        if (processorOptions.isEmpty()) {
            return null;
        }
        return new FunctionItemProcessor<>(processorFunction());
    }

    private Function<KeyValue<K>, KeyValue<K>> processorFunction() {
        return processorOptions.processor(evaluationContext(), codec);
    }

    protected RedisItemReader<String, String> reader() {
        return reader(StringCodec.UTF8);
    }

    protected <K1, V1> RedisItemReader<K1, V1> reader(RedisCodec<K1, V1> codec) {
        return reader(client, codec, readerOptions);
    }

    protected <K1, V1> RedisItemReader<K1, V1> reader(AbstractRedisClient client, RedisCodec<K1, V1> codec,
            RedisReaderOptions options) {
        RedisItemReader<K1, V1> reader = new RedisItemReader<>(client, codec);
        configure(reader, options);
        reader.setKeyProcessor(keyProcessor(codec, options.getKeyFilterOptions()));
        return reader;
    }

    private <K1> ItemProcessor<K1, K1> keyProcessor(RedisCodec<K1, ?> codec, KeyFilterOptions options) {
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
