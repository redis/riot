package com.redislabs.riot;

import java.util.List;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.support.RedisConnectionBuilder;

import com.redislabs.riot.Transfer.TransferListener;

import io.lettuce.core.RedisURI;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarBuilder;
import picocli.CommandLine;

@Slf4j
@CommandLine.Command(abbreviateSynopsis = true, sortOptions = false)
public abstract class AbstractTransferCommand<I, O> extends HelpCommand implements Runnable {

	@CommandLine.ParentCommand
	private RiotApp app;
	@CommandLine.Option(names = "--threads", description = "Thread count (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
	private int threads = 1;
	@CommandLine.Option(names = { "-b",
			"--batch" }, description = "Number of items in each batch (default: ${DEFAULT-VALUE})", paramLabel = "<size>")
	private int batchSize = 50;
	@CommandLine.Option(names = "--max", description = "Max number of items to read", paramLabel = "<count>")
	private Integer maxItemCount;

	protected RedisConnectionOptions getRedisConnectionOptions() {
		return app.getRedisConnectionOptions();
	}

	protected <B extends RedisConnectionBuilder<B>> B configure(RedisConnectionBuilder<B> builder) {
		return app.configure(builder);
	}

	protected <B extends RedisConnectionBuilder<B>> B configure(RedisConnectionBuilder<B> builder,
			RedisConnectionOptions redis) {
		return app.configure(builder, redis);
	}
	
	protected Long flushPeriod() {
		return null;
	}

	@Override
	public void run() {
		List<Transfer<I, O>> transfers;
		try {
			transfers = transfers();
		} catch (Exception e) {
			log.error("Could not create transfer", e);
			return;
		}
		for (Transfer<I, O> transfer : transfers) {
			try {
				execute(transfer);
			} catch (Exception e) {
				log.error("Could not start transfer", e);
				continue;
			}
		}
	}

	public void execute(Transfer<I, O> transfer) throws Exception {
		transfer.open();
		ProgressBarBuilder builder = new ProgressBarBuilder();
		if (transfer.getMaxItemCount() != null) {
			builder.setInitialMax(transfer.getMaxItemCount());
		}
		builder.setTaskName(transfer.getName());
		builder.showSpeed();
		transfer.addListener(TransferProgressMonitor.builder().bar(builder.build()).build());
		try {
			transfer.execute();
		} catch (Exception e) {
			log.error("Could not execute transfer", e);
		} finally {
			transfer.close();
		}
	}

	@Builder
	public static class TransferProgressMonitor implements TransferListener {

		private final ProgressBar bar;

		@Override
		public void onOpen() {
			// do nothing
		}

		@Override
		public void onUpdate(long count) {
			bar.stepTo(count);
		}

		@Override
		public void onClose() {
			bar.close();
		}

	}

	protected abstract List<Transfer<I, O>> transfers() throws Exception;

	protected Transfer<I, O> transfer(String name, ItemReader<I> reader, ItemProcessor<I, O> processor,
			ItemWriter<O> writer) {
		Transfer<I, O> transfer = new Transfer<>(name, reader, processor, writer);
		transfer.setBatchSize(batchSize);
		transfer.setThreadCount(threads);
		transfer.setMaxItemCount(maxItemCount);
		transfer.setFlushPeriod(flushPeriod());
		return transfer;
	}

	protected String toString(RedisURI redisURI) {
		if (redisURI.getSocket() != null) {
			return redisURI.getSocket();
		}
		if (redisURI.getSentinelMasterId() != null) {
			return redisURI.getSentinelMasterId();
		}
		return redisURI.getHost();
	}
}
