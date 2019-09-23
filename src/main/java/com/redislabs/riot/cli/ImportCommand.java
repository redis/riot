package com.redislabs.riot.cli;

import java.util.Map;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command
public abstract class ImportCommand extends TransferCommand {

	@ArgGroup(exclusive = false)
	private RedisWriterOptions redisWriterOptions = new RedisWriterOptions();
	@ArgGroup(exclusive = false, heading = "Processor options%n", order = 40)
	private ProcessorOptions processorOptions = new ProcessorOptions();

	@Override
	protected ItemProcessor<Map<String, Object>, Map<String, Object>> processor() throws Exception {
		return processorOptions.processor(getRedisOptions());
	}

	@Override
	protected ItemWriter<Map<String, Object>> writer() {
		return redisWriterOptions.writer(getRedisOptions());
	}

}
