package com.redis.riot.core;

import com.redis.spring.batch.RedisItemWriter;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.StringCodec;

public abstract class AbstractKeyValueImport extends AbstractJobExecutable {

    private RedisWriterOptions redisWriterOptions = new RedisWriterOptions();

    protected AbstractKeyValueImport(AbstractRedisClient client) {
        super(client);
    }

    public void setRedisWriterOptions(RedisWriterOptions redisWriterOptions) {
        this.redisWriterOptions = redisWriterOptions;
    }

    protected RedisItemWriter<String, String> writer() {
        RedisItemWriter<String, String> writer = new RedisItemWriter<>(client, StringCodec.UTF8);
        redisWriterOptions.configure(writer);
        return writer;
    }

}
