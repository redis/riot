package com.redis.riot.core;

import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.ValueType;

import io.lettuce.core.codec.StringCodec;

public abstract class AbstractKeyValueImport extends AbstractJobExecutable {

    private RedisWriterOptions redisWriterOptions = new RedisWriterOptions();

    public void setRedisWriterOptions(RedisWriterOptions redisWriterOptions) {
        this.redisWriterOptions = redisWriterOptions;
    }

    protected RedisItemWriter<String, String> writer(RiotExecutionContext context) {
        RedisItemWriter<String, String> writer = new RedisItemWriter<>(context.getRedisClient(), StringCodec.UTF8);
        writer.setValueType(ValueType.STRUCT);
        redisWriterOptions.configure(writer);
        return writer;
    }

}
