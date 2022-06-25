package com.redis.riot;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.util.Assert;

import com.redis.riot.redis.EvalCommand;
import com.redis.riot.redis.ExpireCommand;
import com.redis.riot.redis.GeoaddCommand;
import com.redis.riot.redis.HsetCommand;
import com.redis.riot.redis.JsonSetCommand;
import com.redis.riot.redis.LpushCommand;
import com.redis.riot.redis.NoopCommand;
import com.redis.riot.redis.RpushCommand;
import com.redis.riot.redis.SaddCommand;
import com.redis.riot.redis.SetCommand;
import com.redis.riot.redis.SugaddCommand;
import com.redis.riot.redis.TsAddCommand;
import com.redis.riot.redis.XaddCommand;
import com.redis.riot.redis.ZaddCommand;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.writer.RedisOperation;

import io.lettuce.core.codec.StringCodec;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(subcommands = { EvalCommand.class, ExpireCommand.class, GeoaddCommand.class, HsetCommand.class,
		LpushCommand.class, NoopCommand.class, RpushCommand.class, SaddCommand.class, SetCommand.class,
		XaddCommand.class, ZaddCommand.class, SugaddCommand.class, JsonSetCommand.class,
		TsAddCommand.class }, subcommandsRepeatable = true, synopsisSubcommandLabel = "[REDIS COMMAND...]", commandListHeading = "Redis commands:%n")
public abstract class AbstractImportCommand extends AbstractTransferCommand {

	@CommandLine.ArgGroup(exclusive = false, heading = "Processor options%n")
	private MapProcessorOptions processorOptions = new MapProcessorOptions();

	@ArgGroup(exclusive = false, heading = "Writer options%n")
	private RedisWriterOptions writerOptions = new RedisWriterOptions();

	/**
	 * Initialized manually during command parsing
	 */
	private List<RedisCommand<Map<String, Object>>> redisCommands = new ArrayList<>();

	public List<RedisCommand<Map<String, Object>>> getRedisCommands() {
		return redisCommands;
	}

	public void setRedisCommands(List<RedisCommand<Map<String, Object>>> redisCommands) {
		this.redisCommands = redisCommands;
	}

	protected FaultTolerantStepBuilder<Map<String, Object>, Map<String, Object>> step(String name, String taskName,
			ItemReader<Map<String, Object>> reader) throws Exception {
		return step(RiotStep.reader(reader).writer(writer()).processor(processorOptions.processor(getRedisOptions()))
				.name(name).taskName(taskName).max(this::initialMax).build());
	}

	protected Long initialMax() {
		return null;
	}

	private ItemWriter<Map<String, Object>> writer() {
		Assert.notNull(redisCommands, "RedisCommands not set");
		Assert.isTrue(!redisCommands.isEmpty(), "No Redis command specified");
		if (redisCommands.size() == 1) {
			return writer(redisCommands.get(0));
		}
		CompositeItemWriter<Map<String, Object>> compositeWriter = new CompositeItemWriter<>();
		compositeWriter.setDelegates(redisCommands.stream().map(this::writer).collect(Collectors.toList()));
		return compositeWriter;
	}

	private ItemWriter<Map<String, Object>> writer(RedisCommand<Map<String, Object>> command) {
		return writerOptions.configureWriter(writer(command.operation())).build();
	}

	private RedisItemWriter.Builder<String, String, Map<String, Object>> writer(
			RedisOperation<String, String, Map<String, Object>> operation) {
		return writer(getRedisOptions(), StringCodec.UTF8).operation(operation);
	}

}
