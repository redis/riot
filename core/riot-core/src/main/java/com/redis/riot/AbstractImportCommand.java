package com.redis.riot;

import com.redis.riot.redis.EvalCommand;
import com.redis.riot.redis.ExpireCommand;
import com.redis.riot.redis.GeoaddCommand;
import com.redis.riot.redis.HsetCommand;
import com.redis.riot.redis.LpushCommand;
import com.redis.riot.redis.NoopCommand;
import com.redis.riot.redis.RpushCommand;
import com.redis.riot.redis.SaddCommand;
import com.redis.riot.redis.SetCommand;
import com.redis.riot.redis.SugaddCommand;
import com.redis.riot.redis.XaddCommand;
import com.redis.riot.redis.ZaddCommand;
import io.lettuce.core.codec.StringCodec;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.OperationItemWriter;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.util.Assert;
import picocli.CommandLine.Command;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

@Data
@EqualsAndHashCode(callSuper = true)
@Command(subcommands = {EvalCommand.class, ExpireCommand.class, GeoaddCommand.class, HsetCommand.class, LpushCommand.class, NoopCommand.class, RpushCommand.class, SaddCommand.class, SetCommand.class, XaddCommand.class, ZaddCommand.class, SugaddCommand.class}, subcommandsRepeatable = true, synopsisSubcommandLabel = "[REDIS COMMAND...]", commandListHeading = "Redis commands:%n")
public abstract class AbstractImportCommand<I, O> extends AbstractTransferCommand {

    /**
     * Initialized manually during command parsing
     */
    private List<RedisCommand<O>> redisCommands = new ArrayList<>();

    protected FaultTolerantStepBuilder<I, O> step(StepBuilder stepBuilder, String taskName, ItemReader<I> reader) throws Exception {
        RiotStepBuilder<I, O> step = riotStep(stepBuilder, taskName);
        return step.reader(reader).processor(processor()).writer(writer()).build();
    }

    protected abstract ItemProcessor<I, O> processor() throws Exception;

    protected ItemWriter<O> writer() {
        Assert.notNull(redisCommands, "RedisCommands not set");
        Assert.isTrue(!redisCommands.isEmpty(), "No Redis command specified");
        Function<OperationItemWriter.RedisOperation<String, String, O>, ItemWriter<O>> writerProvider = this::writer;
        if (redisCommands.size() == 1) {
            return writerProvider.apply(redisCommands.get(0).operation());
        }
        CompositeItemWriter<O> compositeWriter = new CompositeItemWriter<>();
        compositeWriter.setDelegates(redisCommands.stream().map(RedisCommand::operation).map(writerProvider).collect(Collectors.toList()));
        return compositeWriter;
    }

    private ItemWriter<O> writer(OperationItemWriter.RedisOperation<String, String, O> operation) {
        OperationItemWriter.OperationItemWriterBuilder<String, String, O> writer = OperationItemWriter.operation(operation).codec(StringCodec.UTF8);
        RedisOptions redisOptions = getRedisOptions();
        if (redisOptions.isCluster()) {
            return writer.client(redisOptions.redisClusterClient()).poolConfig(redisOptions.poolConfig()).build();
        }
        return writer.client(redisOptions.redisClient()).poolConfig(redisOptions.poolConfig()).build();
    }


}
