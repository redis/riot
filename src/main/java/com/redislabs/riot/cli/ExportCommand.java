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

	@Override
	public void run() {
		ItemReader<I> reader;
		ItemProcessor<I, O> processor;
		ItemWriter<O> writer;
		try {
			reader = reader(redisOptions());
			processor = processor();
			writer = writer();
		} catch (Exception e) {
			log.error("Could not initialize export", e);
			return;
		}
		execute(reader, processor, writer);
	}

	protected abstract ItemReader<I> reader(RedisOptions redisOptions);

	protected ItemProcessor<I, O> processor() throws Exception {
		return null;
	}

	protected abstract ItemWriter<O> writer() throws Exception;

}
