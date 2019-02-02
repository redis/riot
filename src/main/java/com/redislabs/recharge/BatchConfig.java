package com.redislabs.recharge;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.recharge.RechargeConfiguration.FlowConfiguration;
import com.redislabs.recharge.RechargeConfiguration.NilConfiguration;
import com.redislabs.recharge.RechargeConfiguration.ReaderConfiguration;
import com.redislabs.recharge.RechargeConfiguration.RedisType;
import com.redislabs.recharge.RechargeConfiguration.RedisWriterConfiguration;
import com.redislabs.recharge.RechargeConfiguration.WriterConfiguration;
import com.redislabs.recharge.meter.ProcessorMeter;
import com.redislabs.recharge.meter.ReaderMeter;
import com.redislabs.recharge.meter.WriterMeter;
import com.redislabs.recharge.reader.file.FileConfiguration;
import com.redislabs.recharge.reader.generator.GeneratorEntityItemReader;
import com.redislabs.recharge.writer.redis.AbstractRedisWriter;
import com.redislabs.recharge.writer.redis.FTAddWriter;
import com.redislabs.recharge.writer.redis.GeoAddWriter;
import com.redislabs.recharge.writer.redis.HIncrByWriter;
import com.redislabs.recharge.writer.redis.HMSetWriter;
import com.redislabs.recharge.writer.redis.LPushWriter;
import com.redislabs.recharge.writer.redis.NilWriter;
import com.redislabs.recharge.writer.redis.SAddWriter;
import com.redislabs.recharge.writer.redis.StringWriter;
import com.redislabs.recharge.writer.redis.SuggestionWriter;
import com.redislabs.recharge.writer.redis.XAddWriter;
import com.redislabs.recharge.writer.redis.ZAddWriter;

import lombok.extern.slf4j.Slf4j;

@Component
@EnableBatchProcessing
@Slf4j
@SuppressWarnings("rawtypes")
public class BatchConfig {

	@Autowired
	private JobBuilderFactory jobs;
	@Autowired
	private StepBuilderFactory steps;
	@Autowired
	private RechargeConfiguration config;
	@Autowired
	private FileConfiguration fileConfig;
	@Autowired
	private RediSearchClient client;
	@Autowired
	private JobLauncher jobLauncher;
	@Autowired
	private WriterMeter<Map> writerMeter;
	@Autowired
	private ReaderMeter<Map> readerMeter;
	@Autowired
	private ProcessorMeter<Map, Map> processorMeter;

	private ItemWriter<? super Map> writer(List<WriterConfiguration> writers) {
		if (writers.size() == 0) {
			RedisWriterConfiguration writerConfig = new RedisWriterConfiguration();
			writerConfig.setNil(new NilConfiguration());
			return new NilWriter(client, writerConfig);
		}
		if (writers.size() == 1) {
			return redisWriter(writers.get(0));
		}
		CompositeItemWriter<Map> composite = new CompositeItemWriter<>();
		composite.setDelegates(writers.stream().map(writer -> redisWriter(writer)).collect(Collectors.toList()));
		return composite;
	}

	private AbstractRedisWriter redisWriter(WriterConfiguration writer) {
		RedisWriterConfiguration redis = writer.getRedis();
		switch (redisType(redis)) {
		case nil:
			return new NilWriter(client, redis);
		case string:
			return new StringWriter(client, redis);
		case geo:
			return new GeoAddWriter(client, redis);
		case list:
			return new LPushWriter(client, redis);
		case search:
			return new FTAddWriter(client, redis);
		case suggest:
			return new SuggestionWriter(client, redis);
		case zset:
			return new ZAddWriter(client, redis);
		case set:
			return new SAddWriter(client, redis);
		case stream:
			return new XAddWriter(client, redis);
		default:
			if (redis.getHash() != null && redis.getHash().getIncrby() != null) {
				return new HIncrByWriter(client, redis);
			}
			return new HMSetWriter(client, redis);
		}
	}

	private RedisType redisType(RedisWriterConfiguration redis) {
		if (redis.getType() == null) {
			if (redis.getSet() != null) {
				return RedisType.set;
			}
			if (redis.getGeo() != null) {
				return RedisType.geo;
			}
			if (redis.getList() != null) {
				return RedisType.list;
			}
			if (redis.getNil() != null) {
				return RedisType.nil;
			}
			if (redis.getSearch() != null) {
				return RedisType.search;
			}
			if (redis.getSet() != null) {
				return RedisType.set;
			}
			if (redis.getString() != null) {
				return RedisType.string;
			}
			if (redis.getSuggest() != null) {
				return RedisType.suggest;
			}
			if (redis.getZset() != null) {
				return RedisType.zset;
			}
			if (redis.getStream() != null) {
				return RedisType.stream;
			}
			return RedisType.hash;
		}
		return redis.getType();
	}

	private AbstractItemCountingItemStreamItemReader<Map> reader(ReaderConfiguration reader) throws IOException {
		if (reader.getFile() != null) {
			return fileConfig.reader(reader.getFile());
		}
		if (reader.getGenerator() != null) {
			return new GeneratorEntityItemReader(client, reader.getGenerator());
		}
		return null;
	}

	public void runJob() throws JobExecutionException, InterruptedException, IOException {
		if (config.isFlushall()) {
			flushAll();
		}
		config.getFlows().forEach((k, v) -> {
			try {
				jobLauncher.run(partitionerJob(k, v), new JobParameters());
			} catch (JobExecutionAlreadyRunningException | JobRestartException | JobInstanceAlreadyCompleteException
					| JobParametersInvalidException | IOException e) {
				log.error("Could not run job {}", k, e);
			}
		});
	}

	private Job partitionerJob(String name, FlowConfiguration flow) throws IOException {
		return jobs.get(name + "-job").start(partitionStep(name, flow)).build();
	}

	private Step partitionStep(String name, FlowConfiguration flow) throws IOException {
		return steps.get(name + "-step").partitioner(name + "-slave-step", partitioner(flow))
				.step(slaveStep(name, flow)).taskExecutor(taskExecutor(flow)).build();
	}

	private IndexedPartitioner partitioner(FlowConfiguration flow) {
		return new IndexedPartitioner(flow.getPartitions());
	}

	private TaskExecutor taskExecutor(FlowConfiguration flow) {
		ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
		taskExecutor.setMaxPoolSize(flow.getPartitions());
		taskExecutor.setCorePoolSize(flow.getPartitions());
		taskExecutor.setQueueCapacity(flow.getPartitions());
		taskExecutor.afterPropertiesSet();
		return taskExecutor;
	}

	private Step slaveStep(String name, FlowConfiguration flow) throws IOException {
		SimpleStepBuilder<Map, Map> builder = steps.get(name + "-step").<Map, Map>chunk(flow.getChunkSize());
		AbstractItemCountingItemStreamItemReader<Map> reader = reader(flow.getReader());
		if (flow.getMaxItemCount() > 0) {
			reader.setMaxItemCount(flow.getMaxItemCount());
		}
		reader.setName(name + "-reader");
		builder.reader(reader);
		if (flow.getProcessor() != null) {
			SpelProcessor processor = new SpelProcessor(client.connect(), flow.getProcessor());
			builder.processor(processor);
			builder.listener(processor);
		}
		builder.writer(writer(flow.getWriters()));
		if (config.isMeter()) {
			builder.listener(writerMeter);
			builder.listener(readerMeter);
			builder.listener(processorMeter);
		}
		return builder.build();
	}

	private void flushAll() throws InterruptedException {
		StatefulRediSearchConnection<String, String> connection = client.connect();
		connection.setTimeout(Duration.ofSeconds(10));
		log.warn("***** FLUSHALL in {} millis *****", config.getFlushallWait());
		Thread.sleep(config.getFlushallWait());
		try {
			connection.sync().flushall();
		} finally {
			connection.close();
		}
	}

}
