package com.redis.riot.redis;

import com.redis.riot.RedisOptions;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.DataStructureItemReader;
import org.springframework.batch.item.redis.DataStructureItemWriter;
import org.springframework.batch.item.redis.support.DataStructure;
import org.springframework.batch.item.redis.support.PollableItemReader;
import picocli.CommandLine;

@Slf4j
@CommandLine.Command(name = "replicate-ds", description = "Replicate a source Redis database into a target Redis database using data structure-specific commands")
public class ReplicateDataStructureCommand extends AbstractReplicateCommand<DataStructure> {

    @Override
    protected ItemReader<DataStructure> reader(RedisOptions redisOptions) {
        return dataStructureReader();
    }

    @Override
    protected PollableItemReader<DataStructure> liveReader(RedisOptions redisOptions) {
        if (redisOptions.isCluster()) {
            return configure(DataStructureItemReader.client(redisOptions.redisClusterClient()).live()).build();
        }
        return configure(DataStructureItemReader.client(redisOptions.redisClient()).live()).build();
    }

    @Override
    protected ItemWriter<DataStructure> writer(RedisOptions redisOptions) {
        if (redisOptions.isCluster()) {
            return DataStructureItemWriter.client(redisOptions.redisClusterClient()).poolConfig(redisOptions.poolConfig()).build();
        }
        return DataStructureItemWriter.client(redisOptions.redisClient()).poolConfig(redisOptions.poolConfig()).build();
    }
}
