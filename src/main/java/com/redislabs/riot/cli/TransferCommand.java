package com.redislabs.riot.cli;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;

import com.redislabs.picocliredis.RedisOptions;
import com.redislabs.riot.redis.JedisClusterCommands;
import com.redislabs.riot.redis.JedisPipelineCommands;
import com.redislabs.riot.redis.LettuceAsyncCommands;
import com.redislabs.riot.redis.LettuceReactiveCommands;
import com.redislabs.riot.redis.LettuceSyncCommands;
import com.redislabs.riot.redis.RedisCommands;
import com.redislabs.riot.redis.writer.AbstractCommandWriter;
import com.redislabs.riot.redis.writer.AbstractLettuceItemWriter;
import com.redislabs.riot.redis.writer.AbstractRedisItemWriter;
import com.redislabs.riot.redis.writer.AsyncLettuceItemWriter;
import com.redislabs.riot.redis.writer.ClusterJedisWriter;
import com.redislabs.riot.redis.writer.CommandWriter;
import com.redislabs.riot.redis.writer.PipelineJedisWriter;
import com.redislabs.riot.redis.writer.ReactiveLettuceItemWriter;
import com.redislabs.riot.redis.writer.SyncLettuceItemWriter;
import com.redislabs.riot.redis.writer.map.AbstractSearchMapCommandWriter;
import com.redislabs.riot.transfer.CappedReader;
import com.redislabs.riot.transfer.ErrorHandler;
import com.redislabs.riot.transfer.Flow;
import com.redislabs.riot.transfer.ThrottledReader;
import com.redislabs.riot.transfer.Transfer;
import com.redislabs.riot.transfer.TransferExecution;

import io.lettuce.core.AbstractRedisClient;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Slf4j
@Command
public abstract class TransferCommand<I, O> extends RiotCommand {

	@Option(names = "--threads", description = "Thread count (default: ${DEFAULT-VALUE})", paramLabel = "<count>")
	private int threads = 1;
	@Option(names = { "-b",
			"--batch" }, description = "Number of items in each batch (default: ${DEFAULT-VALUE})", paramLabel = "<size>")
	private int batchSize = 50;
	@Option(names = { "-m", "--max" }, description = "Max number of items to read", paramLabel = "<count>")
	private Long maxItemCount;
	@Option(names = "--sleep", description = "Sleep duration in millis between reads", paramLabel = "<ms>")
	private Long sleep;
	@Option(names = "--progress", description = "Progress reporting interval (default: ${DEFAULT-VALUE} ms)", paramLabel = "<ms>")
	private long progressRate = 300;
	@Option(names = "--max-wait", description = "Max duration to wait for transfer to complete", paramLabel = "<ms>")
	private Long maxWait;

	@Override
	public void run() {
		ItemReader<I> reader;
		try {
			reader = reader();
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
		execute(transfer(reader, processor, writer));
	}

	protected abstract ItemReader<I> reader() throws Exception;

	protected abstract ItemProcessor<I, O> processor() throws Exception;

	protected abstract ItemWriter<O> writer() throws Exception;

	protected Transfer<I, O> transfer(ItemReader<I> reader, ItemProcessor<I, O> processor, ItemWriter<O> writer) {
		return Transfer.<I, O>builder().flow(flow("main", reader, processor, writer)).build();
	}

	protected ErrorHandler errorHandler() {
		return e -> log.error("Could not read item", e);
	}

	protected Flow<I, O> flow(String name, ItemReader<I> reader, ItemProcessor<I, O> processor, ItemWriter<O> writer) {
		return Flow.<I, O>builder().name(name).batchSize(batchSize).nThreads(threads).reader(throttle(cap(reader)))
				.processor(processor).writer(writer).errorHandler(errorHandler()).build();
	}

	protected void execute(Transfer<I, O> transfer) {
		ProgressReporter reporter = progressReporter();
		reporter.start();
		TransferExecution<I, O> execution = transfer.execute();
		ScheduledExecutorService progressReportExecutor = Executors.newSingleThreadScheduledExecutor();
		progressReportExecutor.scheduleAtFixedRate(() -> reporter.onUpdate(execution.progress()), 0, progressRate,
				TimeUnit.MILLISECONDS);
		execution.awaitTermination(maxWait(), TimeUnit.MILLISECONDS);
		progressReportExecutor.shutdown();
		reporter.onUpdate(execution.progress());
		reporter.stop();
	}

	private long maxWait() {
		if (maxWait == null) {
			return Long.MAX_VALUE;
		}
		return maxWait;
	}

	private ItemReader<I> throttle(ItemReader<I> reader) {
		if (sleep == null) {
			return reader;
		}
		return new ThrottledReader<I>().reader(reader).sleep(sleep);
	}

	private ItemReader<I> cap(ItemReader<I> reader) {
		if (maxItemCount == null) {
			return reader;
		}
		return new CappedReader<I>(reader, maxItemCount);
	}

	private ProgressReporter progressReporter() {
		if (isQuiet()) {
			return new NoopProgressReporter();
		}
		ProgressBarReporter progressBarReporter = new ProgressBarReporter().taskName(taskName());
		if (maxItemCount != null) {
			progressBarReporter.initialMax(maxItemCount);
		}
		return progressBarReporter;
	}

	protected abstract String taskName();

	protected AbstractRedisItemWriter<O> writer(RedisOptions redisOptions, CommandWriter<O> writer) {
		if (writer instanceof AbstractCommandWriter) {
			((AbstractCommandWriter<O>) writer).commands(redisCommands(redisOptions));
		}
		if (redisOptions.isJedis()) {
			if (redisOptions.isCluster()) {
				return new ClusterJedisWriter<O>(redisOptions.jedisCluster()).writer(writer);
			}
			return new PipelineJedisWriter<O>(redisOptions.jedisPool()).writer(writer);
		}
		AbstractLettuceItemWriter<O> lettuceWriter = lettuceItemWriter(redisOptions);
		lettuceWriter.writer(writer);
		lettuceWriter.api(writer instanceof AbstractSearchMapCommandWriter ? redisOptions.lettuSearchApi()
				: redisOptions.lettuceApi());
		AbstractRedisClient client = lettuceClient(redisOptions, writer instanceof AbstractSearchMapCommandWriter);
		lettuceWriter.pool(redisOptions.pool(client));
		return lettuceWriter;
	}

	private RedisCommands<?> redisCommands(RedisOptions redis) {
		if (redis.isJedis()) {
			if (redis.isCluster()) {
				return new JedisClusterCommands();
			}
			return new JedisPipelineCommands();
		}
		switch (redis.getApi()) {
		case REACTIVE:
			return new LettuceReactiveCommands();
		case SYNC:
			return new LettuceSyncCommands();
		default:
			return new LettuceAsyncCommands();
		}
	}

	private AbstractLettuceItemWriter<O> lettuceItemWriter(RedisOptions redis) {
		switch (redis.getApi()) {
		case REACTIVE:
			return new ReactiveLettuceItemWriter<>();
		case SYNC:
			return new SyncLettuceItemWriter<>();
		default:
			AsyncLettuceItemWriter<O> lettuceAsyncItemWriter = new AsyncLettuceItemWriter<O>();
			lettuceAsyncItemWriter.timeout(redis.getCommandTimeout());
			return lettuceAsyncItemWriter;
		}
	}

	private AbstractRedisClient lettuceClient(RedisOptions redis, boolean rediSearch) {
		if (rediSearch) {
			return redis.rediSearchClient();
		}
		return redis.lettuceClient();
	}

}
