package com.redis.riot.redis;

import com.redis.riot.RedisOptions;
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
public class ReplicateKeyDumpCommand extends AbstractReplicateCommand<KeyValue<byte[]>> {

    @Override
    protected ItemReader<KeyValue<byte[]>> reader(RedisOptions redisOptions) {
        if (redisOptions.isCluster()) {
            return readerOptions.configure(KeyDumpItemReader.client(redisOptions.clusterClient()).poolConfig(redisOptions.poolConfig())).build();
        }
        return readerOptions.configure(KeyDumpItemReader.client(redisOptions.client()).poolConfig(redisOptions.poolConfig())).build();
    }

    @Override
    protected PollableItemReader<KeyValue<byte[]>> liveReader(RedisOptions redisOptions) {
        if (redisOptions.isCluster()) {
            return configure(KeyDumpItemReader.client(redisOptions.clusterClient()).poolConfig(redisOptions.poolConfig()).live()).build();
        }
        return configure(KeyDumpItemReader.client(redisOptions.client()).poolConfig(redisOptions.poolConfig()).live()).build();
    }

    @Override
    protected ItemWriter<KeyValue<byte[]>> writer(RedisOptions redisOptions) {
        if (redisOptions.isCluster()) {
            return KeyDumpItemWriter.client(redisOptions.clusterClient()).poolConfig(redisOptions.poolConfig()).build();
        }
        return KeyDumpItemWriter.client(redisOptions.client()).poolConfig(redisOptions.poolConfig()).build();
    }
}
