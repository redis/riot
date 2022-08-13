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
import com.redis.spring.batch.writer.Operation;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(subcommands = { EvalCommand.class, ExpireCommand.class, GeoaddCommand.class, HsetCommand.class,
		LpushCommand.class, NoopCommand.class, RpushCommand.class, SaddCommand.class, SetCommand.class,
		XaddCommand.class, ZaddCommand.class, SugaddCommand.class, JsonSetCommand.class,
		TsAddCommand.class }, subcommandsRepeatable = true, synopsisSubcommandLabel = "[REDIS COMMAND...]", commandListHeading = "Redis commands:%n")
public abstract class AbstractImportCommand extends AbstractTransferCommand {

	@ArgGroup(exclusive = false, heading = "Processor options%n")
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

	protected FaultTolerantStepBuilder<Map<String, Object>, Map<String, Object>> step(JobCommandContext context,
			String name, String taskName, ItemReader<Map<String, Object>> reader) {
		Assert.notNull(redisCommands, "RedisCommands not set");
		Assert.isTrue(!redisCommands.isEmpty(), "No Redis command specified");
		ItemWriter<Map<String, Object>> writer = writer(context);
		return step(context,
				RiotStep.reader(reader).writer(writer).processor(processorOptions.processor(context.getRedisClient()))
						.name(name).taskName(taskName).max(this::initialMax).build());
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private ItemWriter<Map<String, Object>> writer(JobCommandContext context) {
		List<ItemWriter<Map<String, Object>>> writers = redisCommands.stream().map(RedisCommand::operation)
				.map(o -> writer(context, o)).collect(Collectors.toList());
		if (writers.size() == 1) {
			return writers.get(0);
		}
		CompositeItemWriter<Map<String, Object>> writer = new CompositeItemWriter<>();
		writer.setDelegates((List) writers);
		return writer;
	}

	private RedisItemWriter<String, String, Map<String, Object>> writer(JobCommandContext context,
			Operation<String, String, Map<String, Object>> operation) {
		return writerOptions.configure(RedisItemWriter.operation(context.getRedisClient(), operation)).build();
	}

	protected Long initialMax() {
		return null;
	}

}
