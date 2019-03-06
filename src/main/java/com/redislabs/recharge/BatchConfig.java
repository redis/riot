package com.redislabs.recharge;

import java.util.List;
import java.util.Map;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemStreamWriter;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.recharge.dummy.DummyReader;
import com.redislabs.recharge.dummy.DummyWriter;
import com.redislabs.recharge.file.FileConfig;
import com.redislabs.recharge.generator.GeneratorConfig;
import com.redislabs.recharge.generator.GeneratorReader;
import com.redislabs.recharge.meter.ProcessorMeter;
import com.redislabs.recharge.meter.ReaderMeter;
import com.redislabs.recharge.meter.WriterMeter;
import com.redislabs.recharge.processor.SpelProcessor;
import com.redislabs.recharge.redis.RedisConfig;

import lombok.extern.slf4j.Slf4j;

@Configuration
@EnableBatchProcessing
@SuppressWarnings("rawtypes")
@Slf4j
public class BatchConfig {

	@Autowired
	private RechargeConfiguration config;
	@Autowired
	private JobBuilderFactory jobs;
	@Autowired
	private StepBuilderFactory steps;
	@Autowired
	private FileConfig file;
	@Autowired
	private GeneratorConfig generator;
	@Autowired
	private RedisConfig redis;
	@Autowired
	private WriterMeter<Map> writerMeter;
	@Autowired
	private ReaderMeter<Map> readerMeter;
	@Autowired
	private ProcessorMeter<Map, Map> processorMeter;
	@Autowired
	private StatefulRediSearchConnection<String, String> connection;

	@Bean
	public Job importJob() throws RechargeException {
		return jobs.get("import-job").start(importStep()).build();
	}

	@Bean
	public Job exportJob() throws RechargeException {
		return jobs.get("export-job").start(exportStep()).build();
	}

	@Bean
	public TaskletStep exportStep() throws RechargeException {
		SimpleStepBuilder<List<String>, List<String>> builder = steps.get("export-step")
				.<List<String>, List<String>>chunk(config.getChunkSize());
		ItemStreamReader<List<String>> reader = redis.reader();
		if (config.getSleep() > 0 || config.getSleepNanos() > 0) {
			reader = new ThrottledItemStreamItemReader<List<String>>(reader, config.getSleep(), config.getSleepNanos());
		}
		builder.reader(reader);
		builder.writer(exportWriter());
		if (config.isMeter()) {
			builder.listener(writerMeter);
			builder.listener(readerMeter);
			builder.listener(processorMeter);
		}
		return builder.build();
	}

	@Bean
	public Step importStep() throws RechargeException {
		TaskletStep taskletStep = importTasklet();
		if (config.getPartitions() > 1) {
			IndexedPartitioner partitioner = new IndexedPartitioner(config.getPartitions());
			return steps.get("import-step").partitioner("delegate-import-step", partitioner).step(taskletStep)
					.taskExecutor(new SimpleAsyncTaskExecutor()).build();
		}
		return taskletStep;
	}

	@Bean
	@StepScope
	public AbstractItemCountingItemStreamItemReader<Map> importReader() throws RechargeException {
		if (config.getFile() != null) {
			AbstractItemCountingItemStreamItemReader<Map> reader = file.reader();
			if (config.getMaxItemCount() > 0) {
				int maxItemCount = config.getMaxItemCount() / config.getPartitions();
				reader.setMaxItemCount(maxItemCount);
			}
			config.getRedis().setKeyspace(file.getBaseName(config.getFile()));
			return reader;
		}
		if (config.getGenerator() != null) {
			GeneratorReader reader = generator.reader();
			reader.setConnection(connection);
			if (config.getMaxItemCount() > 0) {
				int maxItemCount = config.getMaxItemCount() / config.getPartitions();
				reader.setMaxItemCount(maxItemCount);
			}
			return reader;
		}
		return new DummyReader();
	}

	@Bean
	@StepScope
	public ItemStreamWriter<Map> importWriter() throws RechargeException {
		return redis.writer();
	}

	@Bean
	public ItemStreamWriter<List<String>> exportWriter() {
		return new DummyWriter<List<String>>();
	}

	@Bean
	public TaskletStep importTasklet() throws RechargeException {
		SimpleStepBuilder<Map, Map> builder = steps.get("import-tasklet-step").<Map, Map>chunk(config.getChunkSize());
		ItemStreamReader<Map> reader = importReader();
		if (config.getSleep() > 0 || config.getSleepNanos() > 0) {
			reader = new ThrottledItemStreamItemReader<Map>(reader, config.getSleep(), config.getSleepNanos());
		}
		builder.reader(reader);
		if (config.getProcessor() != null) {
			SpelProcessor processor = processor();
			builder.processor(processor);
			builder.listener(new StepExecutionListener() {

				@Override
				public void beforeStep(StepExecution stepExecution) {
					try {
						processor.open();
					} catch (Exception e) {
						log.error("Could not open processor", e);
						stepExecution.addFailureException(e);
					}
				}

				@Override
				public ExitStatus afterStep(StepExecution stepExecution) {
					processor.close();
					return null;
				}
			});
		}
		builder.writer(importWriter());
		if (config.isMeter()) {
			builder.listener(writerMeter);
			builder.listener(readerMeter);
			builder.listener(processorMeter);
		}
		return builder.build();
	}

	@Bean
	@StepScope
	public SpelProcessor processor() {
		SpelProcessor processor = config.getProcessor().processor();
		processor.setConnection(connection);
		return processor;
	}

}
