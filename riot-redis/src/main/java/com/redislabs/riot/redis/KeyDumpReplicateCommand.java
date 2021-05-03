package com.redislabs.riot.redis;

import com.redislabs.riot.RedisOptions;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.KeyDumpItemReader;
import org.springframework.batch.item.redis.KeyDumpItemWriter;
import org.springframework.batch.item.redis.support.KeyValue;
import org.springframework.batch.item.redis.support.PollableItemReader;
import picocli.CommandLine;

@Slf4j
@CommandLine.Command(name = "replicate", description = "Replicate a source Redis database to a target Redis database using DUMP & RESTORE")
public class KeyDumpReplicateCommand extends AbstractReplicateCommand<KeyValue<String, byte[]>> {
    @Override
    protected ItemReader<KeyValue<String, byte[]>> reader(RedisOptions redisOptions) {
        if (redisOptions.isCluster()) {
            return configure(KeyDumpItemReader.client(redisOptions.redisClusterClient()).poolConfig(redisOptions.poolConfig())).build();
        }
        return configure(KeyDumpItemReader.client(redisOptions.redisClient()).poolConfig(redisOptions.poolConfig())).build();
    }

    @Override
    protected PollableItemReader<KeyValue<String, byte[]>> liveReader(RedisOptions redisOptions) {
        if (redisOptions.isCluster()) {
            return configure(KeyDumpItemReader.client(redisOptions.redisClusterClient()).poolConfig(redisOptions.poolConfig()).live()).build();
        }
        return configure(KeyDumpItemReader.client(redisOptions.redisClient()).poolConfig(redisOptions.poolConfig()).live()).build();
    }

    @Override
    protected ItemWriter<KeyValue<String, byte[]>> writer(RedisOptions redisOptions) {
        if (redisOptions.isCluster()) {
            return KeyDumpItemWriter.client(redisOptions.redisClusterClient()).poolConfig(redisOptions.poolConfig()).build();
        }
        return KeyDumpItemWriter.client(redisOptions.redisClient()).poolConfig(redisOptions.poolConfig()).build();
    }
}
