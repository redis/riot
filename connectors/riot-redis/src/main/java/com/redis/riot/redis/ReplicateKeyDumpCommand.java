package com.redis.riot.redis;

import com.redis.riot.RedisOptions;
import io.lettuce.core.AbstractRedisClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.KeyDumpItemReader;
import org.springframework.batch.item.redis.KeyDumpItemWriter;
import org.springframework.batch.item.redis.support.KeyDumpValueReader;
import org.springframework.batch.item.redis.support.KeyValue;
import org.springframework.batch.item.redis.support.PollableItemReader;
import picocli.CommandLine;

@Slf4j
@CommandLine.Command(name = "replicate", description = "Replicate a source Redis database to a target Redis database using DUMP & RESTORE")
public class ReplicateKeyDumpCommand extends AbstractReplicateCommand<KeyValue<byte[]>> {

    @Override
    protected ItemReader<KeyValue<byte[]>> reader(RedisOptions redisOptions) {
        return readerOptions.configure(keyDumpReader(redisOptions.client()).poolConfig(redisOptions.poolConfig())).build();
    }

    private KeyDumpItemReader.KeyDumpItemReaderBuilder keyDumpReader(AbstractRedisClient client) {
        return new KeyDumpItemReader.KeyDumpItemReaderBuilder(client, new KeyDumpValueReader.KeyDumpValueReaderBuilder(client).build());
    }

    @Override
    protected PollableItemReader<KeyValue<byte[]>> liveReader(RedisOptions redisOptions) {
        return configure(keyDumpReader(redisOptions.client()).poolConfig(redisOptions.poolConfig()).live()).build();
    }

    @Override
    protected ItemWriter<KeyValue<byte[]>> writer(RedisOptions redisOptions) {
        return new KeyDumpItemWriter.KeyDumpItemWriterBuilder(redisOptions.client()).poolConfig(redisOptions.poolConfig()).build();
    }

}
