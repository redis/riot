package com.redis.riot.cli.common;

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
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.core.convert.converter.Converter;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.Assert;

import com.redis.lettucemod.util.GeoLocation;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.riot.cli.operation.DelCommand;
import com.redis.riot.cli.operation.EvalCommand;
import com.redis.riot.cli.operation.ExpireCommand;
import com.redis.riot.cli.operation.GeoaddCommand;
import com.redis.riot.cli.operation.HsetCommand;
import com.redis.riot.cli.operation.JsonSetCommand;
import com.redis.riot.cli.operation.LpushCommand;
import com.redis.riot.cli.operation.NoopCommand;
import com.redis.riot.cli.operation.OperationCommand;
import com.redis.riot.cli.operation.RpushCommand;
import com.redis.riot.cli.operation.SaddCommand;
import com.redis.riot.cli.operation.SetCommand;
import com.redis.riot.cli.operation.SugaddCommand;
import com.redis.riot.cli.operation.TsAddCommand;
import com.redis.riot.cli.operation.XaddCommand;
import com.redis.riot.cli.operation.ZaddCommand;
import com.redis.riot.core.convert.RegexNamedGroupsExtractor;
import com.redis.riot.core.processor.CompositeItemStreamItemProcessor;
import com.redis.riot.core.processor.FilteringProcessor;
import com.redis.riot.core.processor.MapAccessor;
import com.redis.riot.core.processor.MapProcessor;
import com.redis.riot.core.processor.SpelProcessor;

import io.lettuce.core.AbstractRedisClient;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(subcommands = { EvalCommand.class, ExpireCommand.class, DelCommand.class, GeoaddCommand.class,
		HsetCommand.class, LpushCommand.class, NoopCommand.class, RpushCommand.class, SaddCommand.class,
		SetCommand.class, XaddCommand.class, ZaddCommand.class, SugaddCommand.class, JsonSetCommand.class,
		TsAddCommand.class }, subcommandsRepeatable = true, synopsisSubcommandLabel = "[REDIS COMMAND...]", commandListHeading = "Redis commands:%n")
public abstract class AbstractImportCommand extends AbstractCommand {

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

	protected ItemProcessor<Map<String, Object>, Map<String, Object>> processor(AbstractRedisClient client) {
		List<ItemProcessor<Map<String, Object>, Map<String, Object>>> processors = new ArrayList<>();
		if (processorOptions.hasSpelFields()) {
			StandardEvaluationContext evaluationContext = new StandardEvaluationContext();
			evaluationContext.setVariable("date", new SimpleDateFormat(processorOptions.getDateFormat()));
			evaluationContext.setVariable("redis", RedisModulesUtils.connection(client).sync());
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
	protected ItemWriter<Map<String, Object>> writer(AbstractRedisClient client) {
		Assert.notNull(redisCommands, "RedisCommands not set");
		Assert.isTrue(!redisCommands.isEmpty(), "No Redis command specified");
		List<ItemWriter<Map<String, Object>>> writers = redisCommands.stream()
				.map(c -> writer(client, writerOptions).operation(c.operation())).collect(Collectors.toList());
		if (writers.size() == 1) {
			return writers.get(0);
		}
		CompositeItemWriter<Map<String, Object>> writer = new CompositeItemWriter<>();
		writer.setDelegates((List) writers);
		return writer;
	}

	protected SimpleStepBuilder<Map<String, Object>, Map<String, Object>> step(AbstractRedisClient client,
			AbstractItemCountingItemStreamItemReader<Map<String, Object>> reader) {
		return step(client, commandName(), reader);
	}

	protected SimpleStepBuilder<Map<String, Object>, Map<String, Object>> step(AbstractRedisClient client, String name,
			ItemReader<Map<String, Object>> reader) {
		SimpleStepBuilder<Map<String, Object>, Map<String, Object>> step = step(name);
		step.reader(reader);
		step.processor(processor(client));
		step.writer(writer(client));
		return step;
	}

}
