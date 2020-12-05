package com.redislabs.riot;

import java.util.List;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.support.BoundedItemReader;
import org.springframework.batch.item.redis.support.MultiTransferExecution;
import org.springframework.batch.item.redis.support.MultiTransferExecutionListenerAdapter;
import org.springframework.batch.item.redis.support.Transfer;
import org.springframework.batch.item.redis.support.TransferExecution;
import org.springframework.batch.item.redis.support.TransferExecutionListener;
import org.springframework.batch.item.redis.support.TransferOptions;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.ClassUtils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulConnection;
import lombok.extern.slf4j.Slf4j;
import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarBuilder;
import picocli.CommandLine.Option;

@Slf4j
public abstract class AbstractTransferCommand<I, O> extends RiotCommand {

	@Option(names = "--threads", description = "Thread count (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
	private int threads = 1;
	@Option(names = { "-b",
			"--batch" }, description = "Number of items in each batch (default: ${DEFAULT-VALUE})", paramLabel = "<size>")
	private int batch = 50;
	@Option(names = "--max", description = "Max number of items to read", paramLabel = "<count>")
	private Integer maxItemCount;

	@Override
	public void run() {
		try {
			execution().start().get();
		} catch (Exception e) {
			log.error("Could not execute command", e);
		}
	}

	public MultiTransferExecution execution() throws Exception {
		RedisOptions redisOptions = getRiotApp().getRedisOptions();
		AbstractRedisClient client = redisOptions.client();
		List<Transfer<I, O>> transfers = transfers(redisOptions.redisURI(), client, redisOptions.poolConfig());
		MultiTransferExecution execution = new MultiTransferExecution(transfers);
		configure(execution);
		execution.addListener(new MultiTransferExecutionListenerAdapter() {
			@Override
			public void onStart(TransferExecution<?, ?> execution) {
				if (getRiotApp().isDisableProgress()) {
					return;
				}
				execution.addListener(new ProgressReporter(execution));
			}

			@Override
			public void onComplete() {
				client.shutdown();
				client.getResources().shutdown();
			}

		});
		return execution;
	}

	protected void configure(MultiTransferExecution execution) {
	}

	private class ProgressReporter implements TransferExecutionListener {

		private final ProgressBar progressBar;

		public ProgressReporter(TransferExecution<?, ?> execution) {
			ProgressBarBuilder builder = new ProgressBarBuilder();
			if (execution.getTransfer().getReader() instanceof BoundedItemReader) {
				builder.setInitialMax(((BoundedItemReader<?>) execution.getTransfer().getReader()).available());
			}
			builder.setTaskName(execution.getTransfer().getName());
			builder.showSpeed();
			this.progressBar = builder.build();
		}

		public void onUpdate(long count) {
			progressBar.stepTo(count);
		}

		@Override
		public void onError(Throwable throwable) {
			log.error("{}: ", throwable);
		}

		@Override
		public void onComplete() {
			progressBar.close();

		}
	}

	protected abstract List<Transfer<I, O>> transfers(RedisURI uri, AbstractRedisClient client,
			GenericObjectPoolConfig<StatefulConnection<String, String>> poolConfig) throws Exception;

	protected Transfer<I, O> transfer(ItemReader<I> reader, ItemProcessor<I, O> processor, ItemWriter<O> writer)
			throws Exception {
		if (maxItemCount != null) {
			if (reader instanceof AbstractItemCountingItemStreamItemReader) {
				log.debug("Configuring reader with maxItemCount={}", maxItemCount);
				((AbstractItemCountingItemStreamItemReader<I>) reader).setMaxItemCount(maxItemCount);
			}
		}
		String readerName = name(reader);
		String name = String.format(transferNameFormat(), readerName);
		return Transfer.<I, O>builder().name(name).reader(reader).processor(processor).writer(writer)
				.options(TransferOptions.builder().batch(batch).threads(threads).build()).build();
	}

	protected abstract String transferNameFormat();

	private String name(ItemReader<I> reader) {
		if (reader instanceof ItemStreamSupport) {
			// this is a hack to get the source name
			String name = ((ItemStreamSupport) reader).getExecutionContextKey("");
			return name.substring(0, name.length() - 1);
		}
		return ClassUtils.getShortName(reader.getClass());

	}

}
