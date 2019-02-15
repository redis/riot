package com.redislabs.recharge;

import java.util.Map;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.SimpleJobBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;

import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.recharge.RechargeConfiguration.FlowConfiguration;
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
	private RedisConfig redis;
	@Autowired
	private WriterMeter<Map> writerMeter;
	@Autowired
	private ReaderMeter<Map> readerMeter;
	@Autowired
	private ProcessorMeter<Map, Map> processorMeter;
	@Autowired
	private StatefulRediSearchConnection<String, String> connection;
	@Autowired
	private TaskExecutor taskExecutor;

	public ItemStreamReader<Map> reader(FlowConfiguration flow) throws RechargeException {
		if (flow.getFile() != null) {
			AbstractItemCountingItemStreamItemReader<Map> reader = flow.getFile().reader();
			if (flow.getMaxItemCount() > 0) {
				int maxItemCount = flow.getMaxItemCount() / flow.getPartitions();
				reader.setMaxItemCount(maxItemCount);
			}
			return reader;
		}
		if (flow.getGenerator() != null) {
			GeneratorReader reader = flow.getGenerator().reader();
			reader.setConnection(connection);
			if (flow.getMaxItemCount() > 0) {
				int maxItemCount = flow.getMaxItemCount() / flow.getPartitions();
				reader.setMaxItemCount(maxItemCount);
			}
			return reader;
		}
		throw new RechargeException("No reader configured");
	}

	public ItemWriter<Map> writer(FlowConfiguration flow) throws RechargeException {
		return redis.writer(flow.getRedis());
	}

	public Job job() throws RechargeException {
		if (config.getFlows().isEmpty()) {
			throw new RechargeException("No flow configured");
		}
		SimpleJobBuilder builder = jobs.get("recharge-job").start(step(config.getFlows().get(0)));
		config.getFlows().subList(1, config.getFlows().size()).forEach(flow -> {
			try {
				builder.next(step(flow));
			} catch (RechargeException e) {
				log.error("Could not create flow {}", flow.getName(), e);
			}
		});
		return builder.build();
	}

	public Step step(FlowConfiguration flow) throws RechargeException {
		TaskletStep taskletStep = taskletStep(flow);
		if (flow.getPartitions() > 1) {
			IndexedPartitioner partitioner = new IndexedPartitioner(flow.getPartitions());
			return steps.get(flow.getName() + "-step").partitioner(flow.getName() + "-delegate-step", partitioner)
					.step(taskletStep).taskExecutor(taskExecutor).build();
		}
		return taskletStep;
	}

	public TaskletStep taskletStep(FlowConfiguration flow) throws RechargeException {
		SimpleStepBuilder<Map, Map> builder = steps.get(flow.getName() + "-step").<Map, Map>chunk(50);
		builder.reader(reader(flow));
		if (flow.getProcessor() != null) {
			SpelProcessor processor = flow.getProcessor().processor();
			processor.setConnection(connection);
			builder.listener(new StepExecutionListener() {

				@Override
				public void beforeStep(StepExecution stepExecution) {
					try {
						processor.open();
					} catch (Exception e) {
						log.error("Could not open processor");
					}
				}

				@Override
				public ExitStatus afterStep(StepExecution stepExecution) {
					processor.close();
					return null;
				}
			});
			builder.processor(processor);
		}
		builder.writer(redis.writer(flow.getRedis()));
		if (config.isMeter()) {
			builder.listener(writerMeter);
			builder.listener(readerMeter);
			builder.listener(processorMeter);
		}
		return builder.build();
	}

}
