package com.redislabs.recharge.batch;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.builder.JobFlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import com.redislabs.recharge.RechargeConfiguration;
import com.redislabs.recharge.RechargeConfiguration.EntityConfiguration;
import com.redislabs.recharge.RechargeException;
import com.redislabs.recharge.file.FileLoadConfiguration;
import com.redislabs.recharge.generator.GeneratorLoadConfig;
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
			log.warn("***** FLUSHALL *****");
			redisTemplate.getConnectionFactory().getConnection().flushAll();
		}
		List<Flow> flows = new ArrayList<>();
		for (EntityConfiguration entityConfig : rechargeConfig.getEntities()) {
			AbstractItemCountingItemStreamItemReader<Map<String, Object>> reader = getReader(entityConfig);
			int entityPosition = rechargeConfig.getEntities().indexOf(entityConfig) + 1;
			String entityId = entityPosition + "-" + entityConfig.getName();
			reader.setName(entityId + "-load-reader");
			if (entityConfig.getName() == null) {
				entityConfig.setName("entity" + entityPosition);
			}
			if (entityConfig.getKeys() == null || entityConfig.getKeys().length == 0) {
				entityConfig.setKeys(entityConfig.getFields());
			}
			SimpleStepBuilder<Map<String, Object>, Map<String, Object>> builder = stepFactory
					.get(entityId + "-load-step")
					.<Map<String, Object>, Map<String, Object>>chunk(entityConfig.getChunkSize());
			builder.listener(stepListener);
			builder.listener(new MeteredItemWriteListener("redis-writer", entityId, metering));
			if (entityConfig.getMaxItemCount() > 0) {
				reader.setMaxItemCount(entityConfig.getMaxItemCount());
			}
			builder.reader(reader);
			builder.writer(redisLoadConfig.getWriter(entityConfig));
			builder.taskExecutor(getTaskExecutor(entityConfig.getMaxThreads()));
			builder.throttleLimit(entityConfig.getMaxThreads() * 2);
			flows.add(new FlowBuilder<Flow>(entityId + "-load-flow").from(builder.build()).end());
		}
		return flows;
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

	private AbstractItemCountingItemStreamItemReader<Map<String, Object>> getReader(EntityConfiguration entity)
			throws Exception {
		if (entity.getGenerator() != null && entity.getGenerator().size() > 0) {
			return generatorLoadConfig.getReader(entity);
		}
		if (entity.getFile() != null) {
			return fileLoadConfig.getReader(entity);
		}
		throw new RechargeException("No entity type configured");
	}

	public Job getLoadJob() throws Exception {
		return getJob("load-job", getLoadFlows());
	}

}
