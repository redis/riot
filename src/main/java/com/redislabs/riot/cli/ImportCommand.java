package com.redislabs.riot.cli;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.CompositeItemProcessor;

import com.redislabs.riot.batch.RegexProcessor;
import com.redislabs.riot.batch.SpelProcessor;
import com.redislabs.riot.cli.redis.RediSearchCommandOptions;
import com.redislabs.riot.cli.redis.RedisCommandOptions;
import com.redislabs.riot.cli.redis.RedisKeyOptions;
import com.redislabs.riot.redis.writer.AbstractRedisItemWriter;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command
public abstract class ImportCommand extends TransferCommand<Map<String, Object>, Map<String, Object>> {

	@Option(names = { "-c",
			"--command" }, description = "Redis command: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<name>")
	private RedisCommand command = RedisCommand.hmset;
	@ArgGroup(exclusive = false, heading = "Redis key options%n")
	private RedisKeyOptions keyOptions = new RedisKeyOptions();
	@ArgGroup(exclusive = false, heading = "Redis command options%n")
	private RedisCommandOptions redis = new RedisCommandOptions();
	@ArgGroup(exclusive = false, heading = "RediSearch command options%n")
	private RediSearchCommandOptions search = new RediSearchCommandOptions();
	@Option(names = { "-p",
			"--processor" }, description = "SpEL expression to process a field", paramLabel = "<name=expression>")
	private Map<String, String> fields;
	@Option(names = { "-r",
			"--regex" }, description = "Extract fields from a source field using a regular expression", paramLabel = "<source=regex>")
	private Map<String, String> regexes;
	@Option(names = "--processor-variable", description = "Register a variable in the processor context", paramLabel = "<name=expression>")
	private Map<String, String> variables = new LinkedHashMap<String, String>();
	@Option(names = "--processor-date-format", description = "java.text.SimpleDateFormat pattern for 'date' processor variable (default: ${DEFAULT-VALUE})", paramLabel = "<string>")
	private String dateFormat = new SimpleDateFormat().toPattern();

	@Override
	protected ItemProcessor<Map<String, Object>, Map<String, Object>> processor() throws Exception {
		if (fields == null) {
			if (regexes == null) {
				return null;
			}
			return regexProcessor();
		}
		if (regexes == null) {
			return spelProcessor();
		}
		CompositeItemProcessor<Map<String, Object>, Map<String, Object>> processor = new CompositeItemProcessor<>();
		processor.setDelegates(Arrays.asList(regexProcessor(), spelProcessor()));
		return processor;
	}

	private ItemProcessor<Map<String, Object>, Map<String, Object>> regexProcessor() {
		return new RegexProcessor(regexes);
	}

	private ItemProcessor<Map<String, Object>, Map<String, Object>> spelProcessor() {
		return new SpelProcessor(getRedisOptions().redis(), new SimpleDateFormat(dateFormat), variables, fields);
	}

	@Override
	protected ItemWriter<Map<String, Object>> writer() {
		AbstractRedisItemWriter<?> writer = itemWriter();
		writer.setConverter(keyOptions.converter());
		return getRedisOptions().writer(writer);
	}

	private AbstractRedisItemWriter<?> itemWriter() {
		switch (command) {
		case ftadd:
			return search.addWriter();
		case ftsugadd:
			return search.sugaddWriter();
		default:
			return redis.writer(command);
		}
	}

}
