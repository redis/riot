package com.redislabs.riot.cli;

import java.net.InetAddress;
import java.text.NumberFormat;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
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
import redis.clients.jedis.Protocol;

public class AbstractCommand<I, O> extends BaseCommand {

	public static final String DEFAULT_HOST = "localhost";

	public enum RedisDriver {
		Jedis, Lettuce
	}

	@Option(names = "--max", description = "Maximum number of items to read.", paramLabel = "<count>")
	private Integer maxCount;
	@Option(names = "--threads", description = "Number of partitions to use for processing. (default: ${DEFAULT-VALUE}).")
	private int threads = 1;
	@Option(names = "--chunk-size", description = "The chunk size commit interval. (default: ${DEFAULT-VALUE}).")
	private int chunkSize = JobBuilder.DEFAULT_CHUNK_SIZE;
	@Option(names = "--sleep", description = "Sleep duration in milliseconds between each read.")
	private Long sleep;
	@Option(names = { "-s", "--host" }, description = "Redis server host. (default: localhost).")
	private InetAddress host;
	@Getter
	@Option(names = { "-h", "--port" }, description = "Redis server port. (default: ${DEFAULT-VALUE}).")
	private int port = RedisURI.DEFAULT_REDIS_PORT;
	@Option(names = "--command-timeout", description = "Redis command timeout in seconds for synchronous command execution (default: ${DEFAULT-VALUE}).")
	private long commandTimeout = RedisURI.DEFAULT_TIMEOUT;
	@Getter
	@Option(names = "--connection-timeout", description = "Redis connect timeout in milliseconds. (default: ${DEFAULT-VALUE}).")
	private int connectionTimeout = Protocol.DEFAULT_TIMEOUT;
	@Option(names = "--socket-timeout", description = "Redis socket timeout in milliseconds. (default: ${DEFAULT-VALUE}).")
	private int socketTimeout = Protocol.DEFAULT_TIMEOUT;
	@Getter
	@Option(names = "--password", description = "Redis database password.", interactive = true)
	protected String password;
	@Option(names = "--max-idle", description = "Maximum number of idle connections in the pool. Use a negative value to indicate an unlimited number of idle connections. (default: ${DEFAULT-VALUE}).")
	private int maxIdle = 8;
	@Option(names = "--min-idle", description = "Target for the minimum number of idle connections to maintain in the pool. This setting only has an effect if it is positive. (default: ${DEFAULT-VALUE}).")
	private int minIdle = 0;
	@Getter
	@Option(names = "--max-total", description = "Maximum number of connections that can be allocated by the pool at a given time. Use a negative value for no limit. (default: ${DEFAULT-VALUE})")
	private int maxTotal = 8;
	@Option(names = "--max-wait", description = "Maximum amount of time in milliseconds a connection allocation should block before throwing an exception when the pool is exhausted. Use a negative value to block indefinitely (default).")
	private long maxWait = -1L;
	@Option(names = "--database", description = "Redis database number. Databases are only available for Redis Standalone and Redis Master/Slave. (default: ${DEFAULT-VALUE}).")
	private int database = 0;
	@Option(names = "--client-name", description = "Redis client name.")
	private String clientName;
	@Getter
	@Option(names = "--driver", description = "Redis driver: ${COMPLETION-CANDIDATES}. (default: ${DEFAULT-VALUE})")
	private RedisDriver driver = RedisDriver.Jedis;

	public RedisConnectionBuilder redisConnectionBuilder() {
		RedisConnectionBuilder builder = new RedisConnectionBuilder();
		builder.setClientName(clientName);
		builder.setCommandTimeout(commandTimeout);
		builder.setConnectionTimeout(connectionTimeout);
		builder.setDatabase(database);
		builder.setHost(getHostname());
		builder.setMaxTotal(maxTotal);
		builder.setMaxIdle(maxIdle);
		builder.setMaxWait(maxWait);
		builder.setMinIdle(minIdle);
		builder.setPassword(password);
		builder.setPort(port);
		builder.setSocketTimeout(socketTimeout);
		return builder;
	}

	public String getHostname() {
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
		System.out.println("Importing " + sourceDescription + " into " + targetDescription);
		SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
		jobLauncher.setJobRepository(builder.jobRepository());
		jobLauncher.setTaskExecutor(new SyncTaskExecutor());
		jobLauncher.afterPropertiesSet();
		JobExecution execution = jobLauncher.run(job, new JobParameters());
		long endTime = System.currentTimeMillis();
		long duration = endTime - startTime;
		NumberFormat numberFormat = NumberFormat.getIntegerInstance();
		double durationInSeconds = (double) duration / 1000;
		int writeCount = execution.getStepExecutions().iterator().next().getWriteCount();
		double throughput = writeCount / durationInSeconds;
		System.out.println("Imported " + numberFormat.format(writeCount) + " items in " + durationInSeconds
				+ " seconds (" + numberFormat.format(throughput) + " writes/sec)");
	}

}
