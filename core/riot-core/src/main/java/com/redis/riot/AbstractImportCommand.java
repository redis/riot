package com.redis.riot;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemProcessor;
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
	private List<OperationCommand<Map<String, Object>>> redisCommands = new ArrayList<>();

	public List<OperationCommand<Map<String, Object>>> getRedisCommands() {
		return redisCommands;
	}

	public void setRedisCommands(List<OperationCommand<Map<String, Object>>> redisCommands) {
		this.redisCommands = redisCommands;
	}

	protected ItemProcessor<Map<String, Object>, Map<String, Object>> processor(JobCommandContext context) {
		return processorOptions.processor(context.getRedisClient());
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected ItemWriter<Map<String, Object>> writer(JobCommandContext context) {
		Assert.notNull(redisCommands, "RedisCommands not set");
		Assert.isTrue(!redisCommands.isEmpty(), "No Redis command specified");
		List<ItemWriter<Map<String, Object>>> writers = redisCommands.stream().map(OperationCommand::operation)
				.map(o -> writerOptions.configure(RedisItemWriter.operation(context.getRedisClient(), o)).build())
				.collect(Collectors.toList());
		if (writers.size() == 1) {
			return writers.get(0);
		}
		CompositeItemWriter<Map<String, Object>> writer = new CompositeItemWriter<>();
		writer.setDelegates((List) writers);
		return writer;
	}

	protected Job job(JobCommandContext context, String name, ItemReader<Map<String, Object>> reader,
			ProgressMonitor monitor) {
		return job(context, name, step(context, name, reader), monitor);
	}

	protected SimpleStepBuilder<Map<String, Object>, Map<String, Object>> step(JobCommandContext context, String name,
			ItemReader<Map<String, Object>> reader) {
		return step(context, name, reader, processor(context), writer(context));
	}

}
