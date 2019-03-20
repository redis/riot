package com.redislabs.recharge;

import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.recharge.db.DatabaseConfig;
import com.redislabs.recharge.file.FileConfig;
import com.redislabs.recharge.generator.GeneratorConfig;
import com.redislabs.recharge.meter.ProcessorMeter;
import com.redislabs.recharge.meter.ReaderMeter;
import com.redislabs.recharge.meter.WriterMeter;
import com.redislabs.recharge.processor.SpelItemProcessor;
import com.redislabs.recharge.redis.RedisConfig;
import com.redislabs.recharge.redis.RedisWriter;

@Configuration
@EnableBatchProcessing
public class BatchConfig {

	@Autowired
	private RechargeConfiguration config;
	@Autowired
	private JobBuilderFactory jobs;
	@Autowired
	private StepBuilderFactory steps;
	@Autowired
	private RedisConfig redis;
	@Autowired
	private DatabaseConfig db;
	@Autowired
	private FileConfig file;
	@Autowired
	private GeneratorConfig generator;
	@Autowired
	private WriterMeter<Map<String, Object>> writerMeter;
	@Autowired
	private ReaderMeter<Map<String, Object>> readerMeter;
	@Autowired
	private ProcessorMeter<Map<String, Object>, Map<String, Object>> processorMeter;

	@Primary
	@Bean
	public DataSource dataSource() {
		return DataSourceBuilder.create().driverClassName("org.h2.Driver").url("jdbc:h2:mem:testdb").build();
	}

	@Bean
	public Job job() throws RechargeException {
		return jobs.get("recharge-job").start(step()).build();
	}

	@Bean
	public Step step() throws RechargeException {
		TaskletStep taskletStep = tasklet();
		if (config.getReader().getPartitions() > 1) {
			IndexedPartitioner partitioner = new IndexedPartitioner(config.getReader().getPartitions());
			return steps.get("import-step").partitioner("delegate-import-step", partitioner).step(taskletStep)
					.taskExecutor(new SimpleAsyncTaskExecutor()).build();
		}
		return taskletStep;
	}

	@Bean
	@StepScope
	public AbstractItemCountingItemStreamItemReader<Map<String, Object>> itemStreamReader() throws RechargeException {
		if (config.getReader().getDb() != null) {
			return db.dbReader();
		}
		if (config.getReader().getFile() != null) {
			return file.reader();
		}
		if (config.getReader().getGenerator() != null) {
			return generator.reader();
		}
		if (config.getReader().getRedis() != null) {
			return redis.reader();
		}
		throw new RechargeException("No reader configured");
	}

	@Bean
	public TaskletStep tasklet() throws RechargeException {
		SimpleStepBuilder<Map<String, Object>, Map<String, Object>> builder = steps.get("import-tasklet-step")
				.<Map<String, Object>, Map<String, Object>>chunk(config.getChunkSize());
		builder.reader(reader());
		if (config.getProcessor() != null) {
			SpelItemProcessor processor = processor(null);
			builder.processor(processor);
			builder.listener(processor);
		}
		builder.writer(writer());
		if (config.isMeter()) {
			builder.listener(writerMeter);
			builder.listener(readerMeter);
			builder.listener(processorMeter);
		}
		return builder.build();
	}

	@Bean
	public ItemReader<Map<String, Object>> reader() throws RechargeException {
		AbstractItemCountingItemStreamItemReader<Map<String, Object>> reader = itemStreamReader();
		if (config.getReader().getMaxItemCountPerPartition() != null) {
			reader.setMaxItemCount(config.getReader().getMaxItemCountPerPartition());
		}
		return throttle(reader);
	}

	@Bean
	@StepScope
	public RedisWriter writer() throws RechargeException {
		return redis.writer();
	}

	private ItemStreamReader<Map<String, Object>> throttle(
			AbstractItemCountingItemStreamItemReader<Map<String, Object>> reader) {
		if (config.getReader().getSleep() > 0 || config.getReader().getSleepNanos() > 0) {
			return new ThrottledItemStreamItemReader<Map<String, Object>>(reader, config.getReader().getSleep(),
					config.getReader().getSleepNanos());
		}
		return reader;
	}

	@Bean
	@StepScope
	public SpelItemProcessor processor(StatefulRediSearchConnection<String, String> connection) {
		SpelItemProcessor processor = config.getProcessor().processor();
		processor.setConnection(connection);
		return processor;
	}

}
