package com.redislabs.riot.cli;

import java.util.Map;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

@Command
public abstract class ImportCommand extends TransferCommand<Map<String, Object>, Map<String, Object>> {

	@Mixin
	private RedisWriterOptions redisWriterOptions = new RedisWriterOptions();
	@ArgGroup(exclusive = false, heading = "Processor options%n")
	private ProcessorOptions processorOptions = new ProcessorOptions();

	@Override
	protected ItemProcessor<Map<String, Object>, Map<String, Object>> processor() throws Exception {
		return processorOptions.processor(getRedisOptions());
	}

	@Override
	protected ItemWriter<Map<String, Object>> writer() {
		return getRedisOptions().writer(redisWriterOptions.writer());
	}

}
