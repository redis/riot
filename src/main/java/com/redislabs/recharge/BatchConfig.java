package com.redislabs.recharge;

import java.util.Map;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.SimpleJobBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;

import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.recharge.RechargeConfiguration.FlowConfiguration;
import com.redislabs.recharge.file.FileConfiguration;
import com.redislabs.recharge.generator.GeneratorEntityItemReader;
import com.redislabs.recharge.meter.ProcessorMeter;
import com.redislabs.recharge.meter.ReaderMeter;
import com.redislabs.recharge.meter.WriterMeter;
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
	private FileConfiguration fileConfig;
	@Autowired
	private RedisConfig redis;
	@Autowired
	private WriterMeter<Map> writerMeter;
	@Autowired
	private ReaderMeter<Map> readerMeter;
	@Autowired
	private ProcessorMeter<Map, Map> processorMeter;
	@Autowired
	private RediSearchClient client;
	@Autowired
	private TaskExecutor taskExecutor;

	public AbstractItemCountingItemStreamItemReader<Map> reader(FlowConfiguration flow) throws RechargeException {
		if (flow.getFile() != null) {
			return fileConfig.reader(flow.getFile());
		}
		if (flow.getGenerator() != null) {
			return new GeneratorEntityItemReader(flow.getGenerator(), client.connect());
		}
		throw new RechargeException("No reader configured");
	}

	public ItemWriter<Map> writer(FlowConfiguration flow) {
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
		AbstractItemCountingItemStreamItemReader<Map> reader = reader(flow);
		if (flow.getMaxItemCount() > 0) {
			reader.setMaxItemCount(flow.getMaxItemCount());
		}
		builder.reader(reader);
		if (flow.getProcessor() != null) {
			SpelProcessor processor = new SpelProcessor(flow.getProcessor(), client.connect());
			builder.processor(processor);
//			builder.listener(new StepExecutionListenerSupport() {
//				@Override
//				public ExitStatus afterStep(StepExecution stepExecution) {
//					log.info("afterStep close");
//					processor.close();
//					return super.afterStep(stepExecution);
//				}
//			});
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
