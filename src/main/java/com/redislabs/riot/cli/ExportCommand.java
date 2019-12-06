package com.redislabs.riot.cli;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;

import com.redislabs.picocliredis.RedisOptions;
import com.redislabs.riot.batch.Transfer;
import com.redislabs.riot.batch.TransferContext;

import picocli.CommandLine.Command;

@Command
public abstract class ExportCommand<I, O> extends TransferCommand<I, O> implements Runnable {

	protected abstract ItemReader<I> reader(RedisOptions redisOptions);

	protected abstract ItemProcessor<I, O> processor() throws Exception;

	protected abstract ItemWriter<O> writer() throws Exception;

	@Override
	public void run() {
		execute(new Transfer<I, O>() {

			@Override
			public ItemReader<I> reader(TransferContext context) throws Exception {
				return ExportCommand.this.reader(ExportCommand.this.redisOptions());
			}

			@Override
			public ItemProcessor<I, O> processor(TransferContext context) throws Exception {
				return ExportCommand.this.processor();
			}

			@Override
			public ItemWriter<O> writer(TransferContext context) throws Exception {
				return ExportCommand.this.writer();
			}

			@Override
			public String unitName() {
				return ExportCommand.this.unitName();
			}

			@Override
			public String taskName() {
				return ExportCommand.this.taskName();
			}

		});
	}

	protected abstract String taskName();

	protected abstract String unitName();
}
