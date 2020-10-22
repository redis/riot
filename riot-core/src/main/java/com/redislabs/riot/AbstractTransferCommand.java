package com.redislabs.riot;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.support.AbstractRedisItemWriter;
import org.springframework.batch.item.redis.support.RedisConnectionBuilder;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;

import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulConnection;
import lombok.extern.slf4j.Slf4j;
import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarBuilder;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;

@Slf4j
@Command(abbreviateSynopsis = true, sortOptions = false)
public abstract class AbstractTransferCommand<I, O> extends HelpCommand {

	@ParentCommand
	private RiotApp app;
	@Option(names = "--threads", description = "Thread count (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
	private int threads = 1;
	@Option(names = { "-b",
			"--batch" }, description = "Number of items in each batch (default: ${DEFAULT-VALUE})", paramLabel = "<size>")
	private int batch = 50;
	@Option(names = "--max", description = "Max number of items to read", paramLabel = "<count>")
	private Integer maxItemCount;

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
			CompletableFuture<Void> future;
			try {
				future = execute(transfer);
			} catch (Exception e) {
				log.error("Could not initialize transfer", e);
				continue;
			}
			future.whenComplete((r, t) -> {
				if (t != null) {
					log.error("Error during transfer", t);
				}
			});
			ProgressBarBuilder builder = new ProgressBarBuilder();
			if (maxItemCount != null) {
				builder.setInitialMax(maxItemCount);
			}
			builder.setTaskName(taskName() + " " + name(transfer.getReader()));
			builder.showSpeed();
			try (ProgressBar progressBar = builder.build()) {
				ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
				ScheduledFuture<?> progressFuture = scheduler
						.scheduleAtFixedRate(() -> progressBar.stepTo(transfer.count()), 0, 100, TimeUnit.MILLISECONDS);
				try {
					future.get();
				} catch (InterruptedException e) {
					// ignore
				} catch (ExecutionException e) {
					log.error("Error during transfer", e);
				} finally {
					scheduler.shutdown();
					progressFuture.cancel(true);
					progressBar.stepTo(transfer.count());
				}
			}
		}
	}

	public CompletableFuture<Void> execute(Transfer<I, O> transfer) {
		return transfer.execute();
	}

	public List<Transfer<I, O>> transfers() throws Exception {
		List<Transfer<I, O>> transfers = new ArrayList<>();
		for (ItemReader<I> reader : readers()) {
			if (maxItemCount != null) {
				if (reader instanceof AbstractItemCountingItemStreamItemReader) {
					log.debug("Configuring reader with maxItemCount={}", maxItemCount);
					((AbstractItemCountingItemStreamItemReader<I>) reader).setMaxItemCount(maxItemCount);
				}
			}
			transfers.add(transfer(reader, processor(), writer()));
		}
		return transfers;
	}

	protected abstract String taskName();

	protected abstract List<ItemReader<I>> readers() throws Exception;

	protected abstract ItemProcessor<I, O> processor() throws Exception;

	protected abstract ItemWriter<O> writer() throws Exception;

	protected Transfer<I, O> transfer(ItemReader<I> reader, ItemProcessor<I, O> processor, ItemWriter<O> writer) {
		return Transfer.<I, O>builder().reader(reader).processor(processor).writer(writer).batch(batch).threads(threads)
				.build();
	}

	private String name(ItemReader<I> reader) {
		if (reader instanceof ItemStreamSupport) {
			// this is a hack to get the source name
			String name = ((ItemStreamSupport) reader).getExecutionContextKey("");
			return name.substring(0, name.length() - 1);
		}
		return "";
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

	protected RedisURI getRedisURI() {
		return app.getRedisConnectionOptions().getRedisURI();
	}

	protected <B extends RedisConnectionBuilder<String, String, B>> B configure(
			RedisConnectionBuilder<String, String, B> builder) {
		return configure(builder, app.getRedisConnectionOptions());
	}

	protected <B extends RedisConnectionBuilder<String, String, B>> B configure(
			RedisConnectionBuilder<String, String, B> builder, RedisConnectionOptions redisConnectionOptions) {
		return redisConnectionOptions.configure(builder);
	}

	protected <T extends AbstractRedisItemWriter<String, String, O>> T configure(T writer) throws Exception {
		return configure(writer, app.getRedisConnectionOptions());
	}

	protected <T extends AbstractRedisItemWriter<String, String, O>> T configure(T writer,
			RedisConnectionOptions redisConnectionOptions) throws Exception {
		RedisConnectionBuilder<String, String, ?> builder = app.connectionBuilder(redisConnectionOptions);
		GenericObjectPool<StatefulConnection<String, String>> pool = builder.pool();
		try (StatefulConnection<String, String> connection = pool.borrowObject()) {
		}
		writer.setPool(pool);
		writer.setCommands(builder.async());
		writer.setCommandTimeout(builder.uri().getTimeout());
		return writer;
	}

}
