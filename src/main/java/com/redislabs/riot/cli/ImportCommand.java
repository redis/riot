package com.redislabs.riot.cli;

import java.util.Map;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;

import com.redislabs.picocliredis.RedisOptions;
import com.redislabs.riot.cli.redis.RedisWriterOptions;

import lombok.Setter;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command
public abstract class ImportCommand extends TransferCommand {

	@Setter
	@ArgGroup(exclusive = false)
	private RedisWriterOptions redisWriterOptions = new RedisWriterOptions();
	@Setter
	@ArgGroup(exclusive = false, heading = "Processor options%n", order = 40)
	private ProcessorOptions processorOptions = new ProcessorOptions();

	@Override
	protected ItemProcessor<Map<String, Object>, Map<String, Object>> processor(RedisOptions options) throws Exception {
		return processorOptions.processor(options);
	}

	@Override
	protected ItemWriter<Map<String, Object>> writer(RedisOptions options) {
		return redisWriterOptions.writer(options);
	}

}
