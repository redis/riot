package com.redis.riot.redis;

import com.redis.riot.AbstractTaskCommand;
import com.redis.riot.RedisOptions;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.BaseRedisCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.util.ClassUtils;
import picocli.CommandLine.Command;

@Command
public abstract class AbstractUtilityCommand extends AbstractTaskCommand {

    @Override
    protected Flow flow(StepBuilderFactory stepBuilderFactory) {

        return flow(stepBuilderFactory.get(ClassUtils.getShortName(getClass()) + "-step").tasklet((contribution, chunkContext) -> {
            RedisOptions redisOptions = getRedisOptions();
            AbstractRedisClient client = redisOptions.redisClient();
            try {
                try (StatefulConnection<String, String> connection = redisOptions.isCluster() ? ((RedisClusterClient) client).connect() : ((RedisClient) client).connect()) {
                    BaseRedisCommands<String, String> commands = redisOptions.isCluster() ? ((StatefulRedisClusterConnection<String, String>) connection).sync() : ((StatefulRedisConnection<String, String>) connection).sync();
                    execute(commands);
                    return RepeatStatus.FINISHED;
                }
            } finally {
                RedisOptions.shutdown(client);
            }
        }).build());
    }

    protected abstract void execute(BaseRedisCommands<String, String> commands);


}
