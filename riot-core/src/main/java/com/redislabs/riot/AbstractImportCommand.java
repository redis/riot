package com.redislabs.riot;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.support.AbstractRedisItemWriter;
import org.springframework.batch.item.support.CompositeItemProcessor;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.util.Assert;

import com.redislabs.riot.processor.MapProcessor;
import com.redislabs.riot.processor.SpelProcessor;
import com.redislabs.riot.redis.AbstractRedisCommand;
import com.redislabs.riot.redis.EvalshaCommand;
import com.redislabs.riot.redis.ExpireCommand;
import com.redislabs.riot.redis.GeoaddCommand;
import com.redislabs.riot.redis.HmsetCommand;
import com.redislabs.riot.redis.LpushCommand;
import com.redislabs.riot.redis.NoopCommand;
import com.redislabs.riot.redis.RpushCommand;
import com.redislabs.riot.redis.SaddCommand;
import com.redislabs.riot.redis.SetCommand;
import com.redislabs.riot.redis.XaddCommand;
import com.redislabs.riot.redis.ZaddCommand;

import lombok.Getter;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(subcommands = { EvalshaCommand.class, ExpireCommand.class, GeoaddCommand.class, HmsetCommand.class,
		LpushCommand.class, NoopCommand.class, RpushCommand.class, SaddCommand.class, SetCommand.class,
		XaddCommand.class,
		ZaddCommand.class }, subcommandsRepeatable = true, synopsisSubcommandLabel = "[REDIS COMMAND]", commandListHeading = "Redis commands:%n")
public abstract class AbstractImportCommand<I, O> extends AbstractTransferCommand<I, O> {

	/*
	 * Initialized manually during command parsing
	 */
	@Getter
	private List<AbstractRedisCommand<O>> redisCommands = new ArrayList<>();

	@CommandLine.Option(arity = "1..*", names = "--spel", description = "SpEL expression to produce a field", paramLabel = "<field=exp>")
	private Map<String, String> spel = new HashMap<>();
	@CommandLine.Option(arity = "1..*", names = "--spel-var", description = "Register a variable in the SpEL processor context", paramLabel = "<v=exp>")
	private Map<String, String> variables = new HashMap<>();
	@CommandLine.Option(arity = "1..*", names = "--regex", description = "Extract named values from source field using regex", paramLabel = "<field=exp>")
	private Map<String, String> regexes = new HashMap<>();
	@CommandLine.Option(names = "--date-format", description = "Processor date format (default: ${DEFAULT-VALUE})", paramLabel = "<string>")
	private String dateFormat = new SimpleDateFormat().toPattern();

	@Override
	protected String taskName() {
		return "Importing from";
	}

	protected ItemProcessor<Map<String, Object>, Map<String, Object>> mapProcessor() {
		List<ItemProcessor<Map<String, Object>, Map<String, Object>>> processors = new ArrayList<>();
		if (!spel.isEmpty()) {
			processors.add(configure(SpelProcessor.builder().dateFormat(new SimpleDateFormat(dateFormat))
					.variables(variables).fields(spel)).build());
		}
		if (!regexes.isEmpty()) {
			processors.add(MapProcessor.builder().regexes(regexes).build());
		}
		if (processors.isEmpty()) {
			return null;
		}
		if (processors.size() == 1) {
			return processors.get(0);
		}
		CompositeItemProcessor<Map<String, Object>, Map<String, Object>> compositeItemProcessor = new CompositeItemProcessor<>();
		compositeItemProcessor.setDelegates(processors);
		return compositeItemProcessor;
	}

	@Override
	protected ItemWriter<O> writer() throws Exception {
		Assert.notNull(redisCommands, "RedisCommands not set");
		List<AbstractRedisItemWriter<String, String, O>> writers = new ArrayList<>();
		for (AbstractRedisCommand<O> redisCommand : redisCommands) {
			writers.add(configure(redisCommand.writer()));
		}
		if (writers.isEmpty()) {
			throw new IllegalArgumentException("No Redis command specified");
		}
		if (writers.size() == 1) {
			return (ItemWriter<O>) writers.get(0);
		}
		CompositeItemWriter<O> writer = new CompositeItemWriter<>();
		writer.setDelegates(new ArrayList<>(writers));
		return writer;
	}

}
