package com.redislabs.riot.cli;

import java.util.Map;

import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemReader;

import com.redislabs.riot.cli.redis.RedisWriterOptions;

import picocli.CommandLine.ArgGroup;

public abstract class ImportCommand extends TransferCommand {

	@ArgGroup(exclusive = false, heading = "Redis writer options%n")
	private RedisWriterOptions writer = new RedisWriterOptions();

	@Override
	public void run() {
		ItemReader<Map<String, Object>> reader;
		try {
			reader = reader();
		} catch (Exception e) {
			LoggerFactory.getLogger(ImportCommand.class).error("Could not initialize reader", e);
			return;
		}
		transfer(reader, writer.writer(redis()));
	}

	protected abstract ItemReader<Map<String, Object>> reader() throws Exception;

}
