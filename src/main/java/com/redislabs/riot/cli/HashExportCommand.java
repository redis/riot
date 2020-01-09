package com.redislabs.riot.cli;

import java.util.Map;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;

import com.redislabs.picocliredis.RedisOptions;
import com.redislabs.riot.cli.redis.HashReaderOptions;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command
public abstract class HashExportCommand extends ExportCommand<Map<String, Object>, Map<String, Object>> {

	@ArgGroup(exclusive = false, heading = "Redis reader options%n")
	private HashReaderOptions readerOptions = new HashReaderOptions();
	@ArgGroup(exclusive = false, heading = "Processor options%n", order = 40)
	private ProcessorOptions processorOptions = new ProcessorOptions();

	@Override
	protected ItemReader<Map<String, Object>> reader(RedisOptions redisOptions) {
		return readerOptions.reader(redisOptions());
	}

	@Override
	protected ItemProcessor<Map<String, Object>, Map<String, Object>> processor() throws Exception {
		return processorOptions.processor(null);
	}

}
