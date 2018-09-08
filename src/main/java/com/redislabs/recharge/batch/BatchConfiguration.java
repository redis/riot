package com.redislabs.recharge.batch;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.builder.JobFlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import com.redislabs.recharge.RechargeConfiguration;
import com.redislabs.recharge.RechargeConfiguration.FlowConfiguration;
import com.redislabs.recharge.RechargeConfiguration.NilConfiguration;
import com.redislabs.recharge.RechargeConfiguration.RedisWriterConfiguration;
import com.redislabs.recharge.RechargeConfiguration.WriterConfiguration;
import com.redislabs.recharge.RechargeException;
import com.redislabs.recharge.file.FileLoadConfiguration;
import com.redislabs.recharge.generator.GeneratorLoadConfig;
import com.redislabs.recharge.redis.AbstractRedisWriter;
import com.redislabs.recharge.redis.NilWriter;
import com.redislabs.recharge.redis.RedisLoadConfiguration;

import lombok.extern.slf4j.Slf4j;

@Component
@EnableBatchProcessing
@Slf4j
public class BatchConfiguration {

	@Autowired
	private RechargeConfiguration rechargeConfig;
	@Autowired
	private JobBuilderFactory jobBuilderFactory;
	@Autowired
	private StepBuilderFactory stepFactory;
	@Autowired
	private FileLoadConfiguration fileLoadConfig;
	@Autowired
	private GeneratorLoadConfig generatorLoadConfig;
	@Autowired
	private RedisLoadConfiguration redisLoadConfig;
	@Autowired
	private StringRedisTemplate redisTemplate;
	@Autowired
	private MeteredJobExecutionListener jobListener;
	@Autowired
	private MeteredStepExecutionListener stepListener;
	@Autowired
	private MeteringProvider metering;

	private List<Flow> getLoadFlows() throws Exception {
		if (rechargeConfig.isFlushall()) {
			log.warn("***** FLUSHALL in {} millis *****", rechargeConfig.getFlushallWait());
			Thread.sleep(rechargeConfig.getFlushallWait());
			redisTemplate.getConnectionFactory().getConnection().flushAll();
		}
		List<Flow> flows = new ArrayList<>();
		for (FlowConfiguration flow : rechargeConfig.getFlows()) {
			int flowPosition = rechargeConfig.getFlows().indexOf(flow) + 1;
			String flowName = "loadflow" + flowPosition;
			String stepName = flowName + "-step";
			SimpleStepBuilder<Map<String, Object>, Map<String, Object>> builder = stepFactory.get(stepName)
					.<Map<String, Object>, Map<String, Object>>chunk(flow.getChunkSize());
			builder.listener(stepListener);
			builder.listener(new MeteredItemWriteListener("redis-writer", flowName, metering));
			AbstractItemCountingItemStreamItemReader<Map<String, Object>> reader = getReader(flow);
			if (flow.getMaxItemCount() > 0) {
				reader.setMaxItemCount(flow.getMaxItemCount());
			}
			reader.setName(flowName + "-reader");
			builder.reader(reader);
			if (flow.getProcessor() != null) {
				builder.processor(getProcessor(flow.getProcessor()));
			}
			builder.writer(getWriter(flow.getWriters()));
			builder.taskExecutor(getTaskExecutor(flow.getMaxThreads()));
			builder.throttleLimit(flow.getMaxThreads() * 2);
			flows.add(new FlowBuilder<Flow>(flowName).from(builder.build()).end());
		}
		return flows;
	}

	private SpelProcessor getProcessor(Map<String, String> processor) {
		return new SpelProcessor(redisTemplate, processor);
	}

	private ItemWriter<? super Map<String, Object>> getWriter(List<WriterConfiguration> writers) {
		if (writers.size() == 0) {
			RedisWriterConfiguration config = new RedisWriterConfiguration();
			config.setNil(new NilConfiguration());
			return new NilWriter(redisTemplate, config);
		}
		if (writers.size() == 1) {
			return getRedisWriter(writers.get(0));
		}
		CompositeItemWriter<Map<String, Object>> composite = new CompositeItemWriter<>();
		composite.setDelegates(writers.stream().map(writer -> getRedisWriter(writer)).collect(Collectors.toList()));
		return composite;
	}

	private AbstractRedisWriter getRedisWriter(WriterConfiguration writer) {
		return redisLoadConfig.getWriter(writer.getRedis());
	}

	private Job getJob(String name, List<Flow> flows) {
		JobBuilder builder = jobBuilderFactory.get(name);
		builder.listener(jobListener);
		if (rechargeConfig.isConcurrent()) {
			Flow flow = new FlowBuilder<Flow>("split-flow").split(getTaskExecutor(flows.size()))
					.add(flows.toArray(new Flow[flows.size()])).build();
			return builder.start(flow).end().build();
		}
		JobFlowBuilder flowBuilder = builder.start(flows.get(0));
		if (flows.size() > 1) {
			for (Flow flow : flows.subList(1, flows.size())) {
				flowBuilder.next(flow);
			}
		}
		return flowBuilder.end().build();
	}

	private TaskExecutor getTaskExecutor(int concurrencyLimit) {
		SimpleAsyncTaskExecutor executor = new SimpleAsyncTaskExecutor();
		executor.setConcurrencyLimit(concurrencyLimit);
		return executor;
	}

	private AbstractItemCountingItemStreamItemReader<Map<String, Object>> getReader(FlowConfiguration flow)
			throws Exception {
		if (flow.getGenerator() != null) {
			return generatorLoadConfig.getReader(flow.getGenerator());
		}
		if (flow.getFile() != null) {
			return fileLoadConfig.getReader(flow.getFile());
		}
		throw new RechargeException("No entity type configured");
	}

	public Job getLoadJob() throws Exception {
		return getJob("load-job", getLoadFlows());
	}

}
