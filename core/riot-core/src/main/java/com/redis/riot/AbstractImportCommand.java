package com.redis.riot;

import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.core.convert.converter.Converter;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.Assert;

import com.redis.lettucemod.util.GeoLocation;
import com.redis.riot.command.EvalCommand;
import com.redis.riot.command.ExpireCommand;
import com.redis.riot.command.GeoaddCommand;
import com.redis.riot.command.HsetCommand;
import com.redis.riot.command.JsonSetCommand;
import com.redis.riot.command.LpushCommand;
import com.redis.riot.command.NoopCommand;
import com.redis.riot.command.RpushCommand;
import com.redis.riot.command.SaddCommand;
import com.redis.riot.command.SetCommand;
import com.redis.riot.command.SugaddCommand;
import com.redis.riot.command.TsAddCommand;
import com.redis.riot.command.XaddCommand;
import com.redis.riot.command.ZaddCommand;
import com.redis.riot.convert.RegexNamedGroupsExtractor;
import com.redis.riot.processor.CompositeItemStreamItemProcessor;
import com.redis.riot.processor.FilteringProcessor;
import com.redis.riot.processor.MapAccessor;
import com.redis.riot.processor.MapProcessor;
import com.redis.riot.processor.SpelProcessor;
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
	private List<OperationCommand<Map<String, Object>>> redisCommands = new ArrayList<>();

	public List<OperationCommand<Map<String, Object>>> getRedisCommands() {
		return redisCommands;
	}

	public MapProcessorOptions getProcessorOptions() {
		return processorOptions;
	}

	public void setProcessorOptions(MapProcessorOptions processorOptions) {
		this.processorOptions = processorOptions;
	}

	public RedisWriterOptions getWriterOptions() {
		return writerOptions;
	}

	public void setWriterOptions(RedisWriterOptions writerOptions) {
		this.writerOptions = writerOptions;
	}

	public void setRedisCommands(List<OperationCommand<Map<String, Object>>> redisCommands) {
		this.redisCommands = redisCommands;
	}

	protected ItemProcessor<Map<String, Object>, Map<String, Object>> processor(JobCommandContext context) {
		List<ItemProcessor<Map<String, Object>, Map<String, Object>>> processors = new ArrayList<>();
		if (processorOptions.hasSpelFields()) {
			StandardEvaluationContext evaluationContext = new StandardEvaluationContext();
			evaluationContext.setVariable("date", new SimpleDateFormat(processorOptions.getDateFormat()));
			evaluationContext.setVariable("redis", context.connection().sync());
			if (processorOptions.hasVariables()) {
				processorOptions.getVariables()
						.forEach((k, v) -> evaluationContext.setVariable(k, v.getValue(evaluationContext)));
			}
			try {
				Method geoMethod = GeoLocation.class.getDeclaredMethod("toString", String.class, String.class);
				evaluationContext.registerFunction("geo", geoMethod);
			} catch (Exception e) {
				throw new UnsupportedOperationException("Could not register geo function", e);
			}
			evaluationContext.setPropertyAccessors(Collections.singletonList(new MapAccessor()));
			processors.add(new SpelProcessor(evaluationContext, processorOptions.getSpelFields()));
		}
		if (processorOptions.hasRegexes()) {
			Map<String, Converter<String, Map<String, String>>> fields = new LinkedHashMap<>();
			processorOptions.getRegexes().forEach((f, r) -> fields.put(f, RegexNamedGroupsExtractor.of(r)));
			processors.add(new MapProcessor(fields));
		}
		if (processorOptions.hasFilters()) {
			processors.add(new FilteringProcessor(processorOptions.getFilters()));
		}
		return CompositeItemStreamItemProcessor.delegates(processors.toArray(new ItemProcessor[0]));
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected ItemWriter<Map<String, Object>> writer(JobCommandContext context) {
		Assert.notNull(redisCommands, "RedisCommands not set");
		Assert.isTrue(!redisCommands.isEmpty(), "No Redis command specified");
		List<ItemWriter<Map<String, Object>>> writers = redisCommands.stream().map(OperationCommand::operation)
				.map(o -> writer(context, o)).collect(Collectors.toList());
		if (writers.size() == 1) {
			return writers.get(0);
		}
		CompositeItemWriter<Map<String, Object>> writer = new CompositeItemWriter<>();
		writer.setDelegates((List) writers);
		return writer;
	}

	private ItemWriter<Map<String, Object>> writer(JobCommandContext context,
			Operation<String, String, Map<String, Object>> operation) {
		return context.writer().options(writerOptions.writerOptions()).operation(operation);
	}

	protected SimpleStepBuilder<Map<String, Object>, Map<String, Object>> step(JobCommandContext context, String name,
			AbstractItemCountingItemStreamItemReader<Map<String, Object>> reader) {
		return step(context, name, reader, processor(context), writer(context));
	}

}
