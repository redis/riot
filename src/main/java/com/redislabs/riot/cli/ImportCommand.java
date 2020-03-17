package com.redislabs.riot.cli;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.springframework.batch.item.ItemProcessor;

import com.redislabs.riot.redis.writer.AbstractRedisItemWriter;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command
public abstract class ImportCommand extends TransferCommand<Map<String, Object>, Map<String, Object>> {

	@ArgGroup(exclusive = false, heading = "Redis command options%n")
	private ImportOptions options = new ImportOptions();
	@ArgGroup(exclusive = false, heading = "Processor options%n")
	private MapProcessorOptions processor = new MapProcessorOptions();

	@Override
	protected AbstractRedisItemWriter<Map<String, Object>> writer() throws Exception {
		return writer(redisOptions(), options.writer());
	}

	@SuppressWarnings("rawtypes")
	@Override
	protected List<ItemProcessor> processors() {
		return Arrays.asList(processor.processor());
	}

}
