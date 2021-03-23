package com.redislabs.riot.redis;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.RedisClusterKeyDumpItemReader;
import org.springframework.batch.item.redis.RedisClusterKeyDumpItemWriter;
import org.springframework.batch.item.redis.RedisKeyDumpItemReader;
import org.springframework.batch.item.redis.RedisKeyDumpItemWriter;
import org.springframework.batch.item.redis.support.KeyValue;
import picocli.CommandLine;

@Slf4j
@CommandLine.Command(name = "replicate", description = "Replicate a source Redis database to a target Redis database using DUMP & RESTORE")
public class KeyDumpReplicateCommand extends AbstractReplicateCommand<KeyValue<String, byte[]>> {

    @Override
    protected ItemReader<KeyValue<String, byte[]>> redisClusterReader(GenericObjectPool<StatefulRedisClusterConnection<String, String>> pool, StatefulRedisClusterConnection<String, String> connection) {
        return configureScanReader(RedisClusterKeyDumpItemReader.builder(pool, connection)).build();
    }

    @Override
    protected ItemReader<KeyValue<String, byte[]>> redisReader(GenericObjectPool<StatefulRedisConnection<String, String>> pool, StatefulRedisConnection<String, String> connection) {
        return configureScanReader(RedisKeyDumpItemReader.builder(pool, connection)).build();
    }

    @Override
    protected ItemReader<KeyValue<String, byte[]>> liveRedisClusterReader(GenericObjectPool<StatefulRedisClusterConnection<String, String>> pool, StatefulRedisClusterPubSubConnection<String, String> pubSubConnection) {
        return configureLiveReader(RedisClusterKeyDumpItemReader.builder(pool, pubSubConnection)).build();
    }

    @Override
    protected ItemReader<KeyValue<String, byte[]>> liveRedisReader(GenericObjectPool<StatefulRedisConnection<String, String>> pool, StatefulRedisPubSubConnection<String, String> pubSubConnection) {
        return configureLiveReader(RedisKeyDumpItemReader.builder(pool, pubSubConnection)).build();
    }

    @Override
    protected ItemWriter<KeyValue<String, byte[]>> redisClusterWriter(GenericObjectPool<StatefulRedisClusterConnection<String, String>> pool, StatefulRedisClusterConnection<String, String> connection) {
        return new RedisClusterKeyDumpItemWriter<>(pool);
    }

    @Override
    protected ItemWriter<KeyValue<String, byte[]>> redisWriter(GenericObjectPool<StatefulRedisConnection<String, String>> pool, StatefulRedisConnection<String, String> connection) {
        return new RedisKeyDumpItemWriter<>(pool);
    }
}
