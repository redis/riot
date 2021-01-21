package com.redislabs.riot;

import com.redislabs.riot.redis.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import lombok.Getter;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.core.step.builder.AbstractTaskletStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.support.CommandItemWriter;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.util.Assert;
import picocli.CommandLine.Command;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

@Command(subcommands = {EvalCommand.class, ExpireCommand.class, GeoaddCommand.class, HmsetCommand.class, LpushCommand.class, NoopCommand.class, RpushCommand.class, SaddCommand.class, SetCommand.class, XaddCommand.class, ZaddCommand.class}, subcommandsRepeatable = true, synopsisSubcommandLabel = "[REDIS COMMAND]", commandListHeading = "Redis commands:%n")
public abstract class AbstractImportCommand<I, O> extends AbstractTransferCommand<I, O> {

    /*
     * Initialized manually during command parsing
     */
    @Getter
    private final List<RedisCommand<O>> redisCommands = new ArrayList<>();

    protected AbstractTaskletStepBuilder<SimpleStepBuilder<I, O>> step(String name, ItemReader<I> reader) {
        StepBuilder<I, O> step = stepBuilder(name);
        return step.reader(reader).processor(processor()).writer(writer()).build();
    }

    protected abstract ItemProcessor<I, O> processor();

    protected ItemWriter<O> writer() {
        Assert.notNull(redisCommands, "RedisCommands not set");
        Assert.isTrue(!redisCommands.isEmpty(), "No Redis command specified");
        Function<RedisCommand<O>, ItemWriter<O>> writerProvider = writerProvider();
        if (redisCommands.size() == 1) {
            return writerProvider.apply(redisCommands.get(0));
        }
        CompositeItemWriter<O> compositeWriter = new CompositeItemWriter<>();
        compositeWriter.setDelegates(redisCommands.stream().map(writerProvider).collect(Collectors.toList()));
        return compositeWriter;
    }

    private Function<RedisCommand<O>, ItemWriter<O>> writerProvider() {
        if (isCluster()) {
            return c -> {
                CommandItemWriter.CommandItemWriterBuilder<O> builder = CommandItemWriter.<O>clusterBuilder((GenericObjectPool<StatefulRedisClusterConnection<String, String>>) pool, (BiFunction) c.command());
                return builder.commandTimeout(getCommandTimeout()).build();
            };
        }
        return c -> {
            CommandItemWriter.CommandItemWriterBuilder<O> builder = CommandItemWriter.<O>builder((GenericObjectPool<StatefulRedisConnection<String, String>>) pool, (BiFunction) c.command());
            return builder.commandTimeout(getCommandTimeout()).build();
        };
    }


}
