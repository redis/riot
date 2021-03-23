package com.redislabs.riot.redis;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.RedisClusterDataStructureItemReader;
import org.springframework.batch.item.redis.RedisClusterDataStructureItemWriter;
import org.springframework.batch.item.redis.RedisDataStructureItemReader;
import org.springframework.batch.item.redis.RedisDataStructureItemWriter;
import org.springframework.batch.item.redis.support.DataStructure;
import picocli.CommandLine;

@Slf4j
@CommandLine.Command(name = "replicate-struct", description = "Replicate a source Redis database into a target Redis database using data structure-specific commands")
public class DataStructureReplicateCommand extends AbstractReplicateCommand<DataStructure<String>> {

    @Override
    protected ItemReader<DataStructure<String>> redisClusterReader(GenericObjectPool<StatefulRedisClusterConnection<String, String>> pool, StatefulRedisClusterConnection<String, String> connection) {
        return configureScanReader(RedisClusterDataStructureItemReader.builder(pool, connection)).build();
    }

    @Override
    protected ItemReader<DataStructure<String>> redisReader(GenericObjectPool<StatefulRedisConnection<String, String>> pool, StatefulRedisConnection<String, String> connection) {
        return configureScanReader(RedisDataStructureItemReader.builder(pool, connection)).build();
    }

    @Override
    protected ItemReader<DataStructure<String>> liveRedisClusterReader(GenericObjectPool<StatefulRedisClusterConnection<String, String>> pool, StatefulRedisClusterPubSubConnection<String, String> pubSubConnection) {
        return configureLiveReader(RedisClusterDataStructureItemReader.builder(pool, pubSubConnection)).build();
    }

    @Override
    protected ItemReader<DataStructure<String>> liveRedisReader(GenericObjectPool<StatefulRedisConnection<String, String>> pool, StatefulRedisPubSubConnection<String, String> pubSubConnection) {
        return configureLiveReader(RedisDataStructureItemReader.builder(pool, pubSubConnection)).build();
    }

    @Override
    protected ItemWriter<DataStructure<String>> redisClusterWriter(GenericObjectPool<StatefulRedisClusterConnection<String, String>> pool, StatefulRedisClusterConnection<String, String> connection) {
        return new RedisClusterDataStructureItemWriter<>(pool);
    }

    @Override
    protected ItemWriter<DataStructure<String>> redisWriter(GenericObjectPool<StatefulRedisConnection<String, String>> pool, StatefulRedisConnection<String, String> connection) {
        return new RedisDataStructureItemWriter<>(pool);
    }
}
