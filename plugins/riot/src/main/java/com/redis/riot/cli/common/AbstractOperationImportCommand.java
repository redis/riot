package com.redis.riot.cli.common;

import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.core.convert.converter.Converter;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.Assert;

import com.redis.lettucemod.util.GeoLocation;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.riot.cli.operation.DelCommand;
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
import com.redis.spring.batch.writer.OperationItemWriter;
import com.redis.spring.batch.writer.OperationItemWriter.Builder;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(subcommands = { ExpireCommand.class, DelCommand.class, GeoaddCommand.class, HsetCommand.class,
		LpushCommand.class, NoopCommand.class, RpushCommand.class, SaddCommand.class, SetCommand.class,
		XaddCommand.class, ZaddCommand.class, SugaddCommand.class, JsonSetCommand.class,
		TsAddCommand.class }, subcommandsRepeatable = true, synopsisSubcommandLabel = "[REDIS COMMAND...]", commandListHeading = "Redis commands:%n")
public abstract class AbstractOperationImportCommand extends AbstractImportCommand {

	@ArgGroup(exclusive = false, heading = "Processor options%n")
	protected MapProcessorOptions processorOptions = new MapProcessorOptions();

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

	private ItemProcessor<Map<String, Object>, Map<String, Object>> processor(CommandContext context) {
		List<ItemProcessor<Map<String, Object>, Map<String, Object>>> processors = new ArrayList<>();
		if (processorOptions.hasSpelFields()) {
			StandardEvaluationContext evaluationContext = new StandardEvaluationContext();
			evaluationContext.setVariable("date", new SimpleDateFormat(processorOptions.getDateFormat()));
			evaluationContext.setVariable("redis", RedisModulesUtils.connection(context.getRedisClient()).sync());
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

	protected RiotStep<Map<String, Object>, Map<String, Object>> step(CommandContext context,
			ItemReader<Map<String, Object>> reader) {
		return step(reader, writer(context)).processor(processor(context));
	}

	private ItemWriter<Map<String, Object>> writer(CommandContext context) {
		Assert.notNull(redisCommands, "RedisCommands not set");
		Assert.isTrue(!redisCommands.isEmpty(), "No Redis command specified");
		Builder<String, String> writer = OperationItemWriter.builder(context.getRedisClient());
		configure(writer);
		return writer(redisCommands.stream().map(OperationCommand::operation).map(writer::build)
				.collect(Collectors.toList()));
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private <T> ItemWriter<T> writer(List<ItemWriter<T>> writers) {
		if (writers.isEmpty()) {
			throw new IllegalArgumentException("At least one writer must be given");
		}
		if (writers.size() == 1) {
			return writers.get(0);
		}
		CompositeItemWriter<T> writer = new CompositeItemWriter<>();
		writer.setDelegates((List) writers);
		return writer;
	}

}
