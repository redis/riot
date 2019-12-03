package com.redislabs.riot.cli;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;

import com.redislabs.picocliredis.RedisOptions;

import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine.Command;

@Slf4j
@Command
public abstract class ExportCommand<I, O> extends TransferCommand<I, O> implements Runnable {

	protected abstract ItemReader<I> reader(RedisOptions redisOptions);

	protected abstract ItemProcessor<I, O> processor() throws Exception;

	protected abstract ItemWriter<O> writer() throws Exception;

	@Override
	public void run() {
		ItemReader<I> reader;
		try {
			reader = reader(redisOptions());
		} catch (Exception e) {
			log.error("Could not initialize reader", e);
			return;
		}
		ItemProcessor<I, O> processor;
		try {
			processor = processor();
		} catch (Exception e) {
			log.error("Could not initialize processor", e);
			return;
		}
		ItemWriter<O> writer;
		try {
			writer = writer();
		} catch (Exception e) {
			log.error("Could not initialize writer", e);
			return;
		}
		execute(reader, processor, writer);
	}
}
