package com.redislabs.riot.cli;

import java.util.Map;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;

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
	@ArgGroup(exclusive = false, heading = "Processor options%n")
	private ProcessorOptions processorOptions = new ProcessorOptions();

	@Override
	protected ItemProcessor<Map<String, Object>, Map<String, Object>> processor() throws Exception {
		return processorOptions.processor(getRedisOptions());
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
