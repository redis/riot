package com.redis.riot;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.util.Assert;

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
import com.redis.spring.batch.RedisItemWriter.RedisItemWriterBuilder;
import com.redis.spring.batch.support.RedisOperation;

import lombok.Data;
import lombok.EqualsAndHashCode;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Data
@EqualsAndHashCode(callSuper = true)
@Command(subcommands = { EvalCommand.class, ExpireCommand.class, GeoaddCommand.class, HsetCommand.class,
		LpushCommand.class, NoopCommand.class, RpushCommand.class, SaddCommand.class, SetCommand.class,
		XaddCommand.class, ZaddCommand.class,
		SugaddCommand.class }, subcommandsRepeatable = true, synopsisSubcommandLabel = "[REDIS COMMAND...]", commandListHeading = "Redis commands:%n")
public abstract class AbstractImportCommand<I, O> extends AbstractTransferCommand {

	@ArgGroup(exclusive = false, heading = "Writer options%n")
	private RedisWriterOptions writerOptions = new RedisWriterOptions();

	/**
	 * Initialized manually during command parsing
	 */
	private List<RedisCommand<O>> redisCommands = new ArrayList<>();

	protected FaultTolerantStepBuilder<I, O> step(String name, String taskName, ItemReader<I> reader) throws Exception {
		RiotStepBuilder<I, O> step = riotStep(name, taskName);
		return step.reader(reader).processor(processor()).writer(writer()).build();
	}

	private ItemWriter<O> writer() {
		Assert.notNull(redisCommands, "RedisCommands not set");
		Assert.isTrue(!redisCommands.isEmpty(), "No Redis command specified");
		if (redisCommands.size() == 1) {
			return writer(redisCommands.get(0));
		}
		CompositeItemWriter<O> compositeWriter = new CompositeItemWriter<>();
		compositeWriter.setDelegates(redisCommands.stream().map(this::writer).collect(Collectors.toList()));
		return compositeWriter;
	}

	protected abstract ItemProcessor<I, O> processor() throws Exception;

	private ItemWriter<O> writer(RedisCommand<O> command) {
		return writerOptions.configureWriter(writer(command.operation())).build();
	}

	private RedisItemWriterBuilder<String, String, O> writer(RedisOperation<String, String, O> operation) {
		return writer(getRedisOptions()).operation(operation);
	}

}
