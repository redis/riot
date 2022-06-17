package com.redis.riot;

import java.time.Duration;
import java.util.concurrent.ThreadPoolExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.skip.AlwaysSkipItemSkipPolicy;
import org.springframework.batch.core.step.skip.NeverSkipItemSkipPolicy;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.redis.riot.TransferOptions.Progress;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisItemReader.TypeBuilder;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.RedisItemWriter.OperationBuilder;
import com.redis.spring.batch.RedisScanSizeEstimator;
import com.redis.spring.batch.reader.PollableItemReader;

import io.lettuce.core.codec.RedisCodec;
import picocli.CommandLine;

public abstract class AbstractTransferCommand extends AbstractRiotCommand {

	private static final Logger log = LoggerFactory.getLogger(AbstractTransferCommand.class);

	@CommandLine.Mixin
	protected TransferOptions transferOptions = new TransferOptions();

	protected TypeBuilder<String, String> stringReader(RedisOptions redisOptions) {
		return RedisItemReader.client(redisOptions.client()).string();
	}

	protected <K, V> TypeBuilder<K, V> reader(RedisOptions redisOptions, RedisCodec<K, V> codec) {
		return RedisItemReader.client(redisOptions.client()).codec(codec);
	}

	protected <K, V> OperationBuilder<K, V> writer(RedisOptions redisOptions, RedisCodec<K, V> codec) {
		return RedisItemWriter.client(redisOptions.client()).codec(codec);
	}

	protected RedisScanSizeEstimator.Builder estimator() {
		return RedisScanSizeEstimator.client(getRedisOptions().client());
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public <I, O> FaultTolerantStepBuilder<I, O> step(RiotStep<I, O> riotStep) throws Exception {
		SimpleStepBuilder<I, O> step = chunk(riotStep.getName(), transferOptions.getChunkSize());
		step.reader(riotStep.getReader()).writer(riotStep.getWriter());
		riotStep.getProcessor().ifPresent(step::processor);
		if (transferOptions.getProgress() != Progress.NONE) {
			ProgressMonitor.ProgressMonitorBuilder monitorBuilder = ProgressMonitor.builder();
			monitorBuilder.style(transferOptions.getProgress());
			monitorBuilder.taskName(riotStep.getTaskName());
			monitorBuilder.initialMax(riotStep.getMax());
			monitorBuilder.updateInterval(Duration.ofMillis(transferOptions.getProgressUpdateIntervalMillis()));
			monitorBuilder.extraMessage(riotStep.getMessage());
			ProgressMonitor monitor = monitorBuilder.build();
			step.listener((StepExecutionListener) monitor);
			step.listener((ItemWriteListener) monitor);
		}
		FaultTolerantStepBuilder<I, O> ftStep = step.faultTolerant()
				.skipPolicy(skipPolicy(transferOptions.getSkipPolicy()));
		if (transferOptions.getThreads() > 1) {
			ftStep.reader(synchronize(riotStep.getReader()));
			ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
			taskExecutor.setCorePoolSize(transferOptions.getThreads());
			taskExecutor.setMaxPoolSize(transferOptions.getThreads());
			taskExecutor.setQueueCapacity(transferOptions.getThreads());
			taskExecutor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
			taskExecutor.afterPropertiesSet();
			log.debug("Created pooled task executor of size {}", taskExecutor.getCorePoolSize());
			ftStep.taskExecutor(taskExecutor).throttleLimit(transferOptions.getThreads());
		} else {
			ftStep.taskExecutor(new SyncTaskExecutor());
		}
		return ftStep;
	}

	protected <I, O> SimpleStepBuilder<I, O> chunk(String name, int chunkSize) throws Exception {
		return getJobRunner().step(name).chunk(chunkSize);
	}

	private <I> ItemReader<I> synchronize(ItemReader<I> reader) {
		if (reader instanceof PollableItemReader) {
			SynchronizedPollableItemReader<I> pollableReader = new SynchronizedPollableItemReader<>();
			pollableReader.setDelegate((PollableItemReader<I>) reader);
			return pollableReader;
		}
		if (reader instanceof ItemStreamReader) {
			SynchronizedItemStreamReader<I> streamReader = new SynchronizedItemStreamReader<>();
			streamReader.setDelegate((ItemStreamReader<I>) reader);
			return streamReader;
		}
		return reader;
	}

	protected <I, O> FaultTolerantStepBuilder<I, O> faultTolerant(SimpleStepBuilder<I, O> step) {
		return step.faultTolerant();
	}

	private SkipPolicy skipPolicy(TransferOptions.SkipPolicy policy) {
		switch (policy) {
		case ALWAYS:
			return new AlwaysSkipItemSkipPolicy();
		case NEVER:
			return new NeverSkipItemSkipPolicy();
		default:
			return RedisItemReader.limitCheckingSkipPolicy(transferOptions.getSkipLimit());
		}
	}

}
