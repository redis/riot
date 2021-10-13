package com.redis.riot.redis;

import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.DataStructureItemReader;
import org.springframework.batch.item.redis.DataStructureItemWriter;
import org.springframework.batch.item.redis.support.DataStructure;
import org.springframework.batch.item.redis.support.DataStructureValueReader;
import org.springframework.batch.item.redis.support.PollableItemReader;

import com.redis.riot.RedisOptions;

import io.lettuce.core.AbstractRedisClient;
import picocli.CommandLine;

@CommandLine.Command(name = "replicate-ds", description = "Replicate a source Redis database into a target Redis database using data structure-specific commands")
public class ReplicateDataStructureCommand extends AbstractReplicateCommand<DataStructure> {

    @Override
    protected ItemReader<DataStructure> reader(RedisOptions redisOptions) {
        return dataStructureReader();
    }

    @Override
    protected PollableItemReader<DataStructure> liveReader(RedisOptions redisOptions) {
        AbstractRedisClient client = redisOptions.client();
        return configure(new DataStructureItemReader.DataStructureItemReaderBuilder(client, new DataStructureValueReader.DataStructureValueReaderBuilder(client).build()).live()).build();
    }

    @Override
    protected ItemWriter<DataStructure> writer(RedisOptions redisOptions) {
        return new DataStructureItemWriter.DataStructureItemWriterBuilder(redisOptions.client()).poolConfig(redisOptions.poolConfig()).build();
    }
}
