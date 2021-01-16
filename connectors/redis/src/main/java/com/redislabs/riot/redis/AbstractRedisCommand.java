package com.redislabs.riot.redis;

import com.redislabs.riot.AbstractTaskCommand;
import io.lettuce.core.api.sync.BaseRedisCommands;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.util.ClassUtils;
import picocli.CommandLine.Command;

@Command
public abstract class AbstractRedisCommand extends AbstractTaskCommand {

    @Override
    protected Flow flow() {
        return flow(step(ClassUtils.getShortName(getClass())).tasklet((contribution, chunkContext) -> {
            execute(sync());
            return RepeatStatus.FINISHED;
        }).build());
    }

    protected abstract void execute(BaseRedisCommands<String, String> commands);


}
