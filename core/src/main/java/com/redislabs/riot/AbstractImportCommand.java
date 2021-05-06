package com.redislabs.riot;

import com.redislabs.riot.redis.*;
import lombok.Getter;
import lombok.Setter;
import org.springframework.batch.core.step.builder.AbstractTaskletStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.OperationItemWriter;
import org.springframework.batch.item.redis.RedisOperation;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.util.Assert;
import picocli.CommandLine.Command;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

@Command(subcommands = {EvalCommand.class, ExpireCommand.class, GeoaddCommand.class, HsetCommand.class, LpushCommand.class, NoopCommand.class, RpushCommand.class, SaddCommand.class, SetCommand.class, XaddCommand.class, ZaddCommand.class, SugaddCommand.class}, subcommandsRepeatable = true, synopsisSubcommandLabel = "[REDIS COMMAND]", commandListHeading = "Redis commands:%n")
public abstract class AbstractImportCommand<I, O> extends AbstractTransferCommand {

    /**
     * Initialized manually during command parsing
     */
    @Setter
    @Getter
    private List<RedisCommand<O>> redisCommands = new ArrayList<>();

    protected AbstractTaskletStepBuilder<SimpleStepBuilder<I, O>> step(StepBuilder stepBuilder, String taskName, ItemReader<I> reader) throws Exception {
        RiotStepBuilder<I, O> step = riotStep(stepBuilder, taskName);
        return step.reader(reader).processor(processor()).writer(writer()).build();
    }

    protected abstract ItemProcessor<I, O> processor() throws Exception;

    protected ItemWriter<O> writer() {
        Assert.notNull(redisCommands, "RedisCommands not set");
        Assert.isTrue(!redisCommands.isEmpty(), "No Redis command specified");
        Function<RedisOperation<String, String, O>, ItemWriter<O>> writerProvider = this::writer;
        if (redisCommands.size() == 1) {
            return writerProvider.apply(redisCommands.get(0).operation());
        }
        CompositeItemWriter<O> compositeWriter = new CompositeItemWriter<>();
        compositeWriter.setDelegates(redisCommands.stream().map(RedisCommand::operation).map(writerProvider).collect(Collectors.toList()));
        return compositeWriter;
    }

    private ItemWriter<O> writer(RedisOperation<String, String, O> operation) {
        OperationItemWriter.OperationItemWriterBuilder<O> writer = OperationItemWriter.operation(operation);
        RedisOptions redisOptions = getRedisOptions();
        if (redisOptions.isCluster()) {
            return writer.client(redisOptions.redisClusterClient()).poolConfig(redisOptions.poolConfig()).build();
        }
        return writer.client(redisOptions.redisClient()).poolConfig(redisOptions.poolConfig()).build();
    }


}
