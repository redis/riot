package com.redislabs.riot.cli;

import java.net.InetAddress;
import java.text.NumberFormat;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStreamWriter;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.core.task.SyncTaskExecutor;

import com.redislabs.riot.batch.JobBuilder;
import com.redislabs.riot.redis.RedisConnectionBuilder;

import io.lettuce.core.RedisURI;
import lombok.Getter;
import picocli.CommandLine.Option;

public class AbstractCommand<I, O> extends HelpAwareCommand {

	public static final String DEFAULT_HOST = "localhost";

	@Option(names = "--max", description = "Maximum number of items to read.", paramLabel = "<count>")
	@Getter
	private Integer maxCount;
	@Option(names = "--threads", description = "Number of partitions to use for processing. (default: ${DEFAULT-VALUE}).")
	@Getter
	private int threads = 1;
	@Option(names = "--chunk-size", description = "The chunk size commit interval. (default: ${DEFAULT-VALUE}).")
	@Getter
	private int chunkSize = JobBuilder.DEFAULT_CHUNK_SIZE;
	@Option(names = "--sleep", description = "Sleep duration in milliseconds between each read.")
	@Getter
	private Long sleep;
	@Option(names = { "-h", "--host" }, description = "Redis server host (default: localhost).", order = 1)
	private InetAddress host;
	@Option(names = { "-p", "--port" }, description = "Redis server port (default: ${DEFAULT-VALUE}).", order = 2)
	private int port = RedisURI.DEFAULT_REDIS_PORT;
	@Option(names = "--timeout", description = "Redis command timeout in seconds for synchronous command execution (default: ${DEFAULT-VALUE}).", order = 3)
	private long timeout = RedisURI.DEFAULT_TIMEOUT;
	@Option(names = "--password", description = "Redis database password.", interactive = true, order = 3)
	private String password;
	@Option(names = "--max-idle", description = "Maximum number of idle connections in the pool (default: ${DEFAULT-VALUE}). Use a negative value to indicate an unlimited number of idle connections.", order = 3)
	private int maxIdle = 8;
	@Option(names = "--min-idle", description = "Target for the minimum number of idle connections to maintain in the pool (default: ${DEFAULT-VALUE}). This setting only has an effect if it is positive.", order = 3)
	private int minIdle = 0;
	@Option(names = "--max-active", description = "Maximum number of connections that can be allocated by the pool at a given time (default: ${DEFAULT-VALUE}). Use a negative value for no limit.", order = 3)
	private int maxActive = 8;
	@Option(names = "--max-wait", description = "Maximum amount of time in milliseconds a connection allocation should block before throwing an exception when the pool is exhausted. Use a negative value to block indefinitely (default).", order = 3)
	private long maxWait = -1L;

	public RedisConnectionBuilder redisConnectionBuilder() {
		RedisConnectionBuilder builder = new RedisConnectionBuilder();
		builder.setHost(getHostname());
		builder.setMaxActive(maxActive);
		builder.setMaxIdle(maxIdle);
		builder.setMaxWait(maxWait);
		builder.setMinIdle(minIdle);
		builder.setPassword(password);
		builder.setPort(port);
		builder.setTimeout(timeout);
		return builder;
	}

	private String getHostname() {
		if (host != null) {
			return host.getHostName();
		}
		return DEFAULT_HOST;
	}

	public void run(String sourceDescription, AbstractItemCountingItemStreamItemReader<I> reader,
			ItemProcessor<I, O> processor, String targetDescription, ItemStreamWriter<O> writer) throws Exception {
		JobBuilder<I, O> builder = new JobBuilder<>();
		builder.setChunkSize(chunkSize);
		if (maxCount != null) {
			reader.setMaxItemCount(maxCount);
		}
		builder.setReader(reader);
		builder.setProcessor(processor);
		builder.setWriter(writer);
		builder.setPartitions(threads);
		builder.setSleep(sleep);
		Job job = builder.build();
		long startTime = System.currentTimeMillis();
		System.out.println("Importing into " + targetDescription + " from " + sourceDescription);
		SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
		jobLauncher.setJobRepository(builder.jobRepository());
		jobLauncher.setTaskExecutor(new SyncTaskExecutor());
		jobLauncher.afterPropertiesSet();
		JobExecution execution = jobLauncher.run(job, new JobParameters());
		long endTime = System.currentTimeMillis();
		long duration = endTime - startTime;
		NumberFormat numberFormat = NumberFormat.getIntegerInstance();
		for (StepExecution stepExecution : execution.getStepExecutions()) {
			double durationInSeconds = (double) duration / 1000;
			int writeCount = stepExecution.getWriteCount();
			double throughput = writeCount / durationInSeconds;
			System.out.println("Imported " + numberFormat.format(writeCount) + " items in " + durationInSeconds
					+ " seconds (" + numberFormat.format(throughput) + " writes/sec)");
		}
	}

}
