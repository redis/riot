package com.redislabs.recharge.batch;

import java.util.Arrays;
import java.util.Map;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

import com.redislabs.recharge.dummy.DummyStep;
import com.redislabs.recharge.dummy.DummyItemWriter;
import com.redislabs.recharge.file.FileConfiguration;
import com.redislabs.recharge.file.LoadFileStep;
import com.redislabs.recharge.generator.GeneratorConfiguration;
import com.redislabs.recharge.generator.LoadGeneratorStep;
import com.redislabs.recharge.redis.HashItem;
import com.redislabs.recharge.redis.RediSearchConfiguration;
import com.redislabs.recharge.redis.RediSearchWriter;
import com.redislabs.recharge.redis.RedisWriter;

@Configuration
@EnableBatchProcessing
public class LoadJobConfiguration {

	@Autowired
	private RechargeConfiguration config;

	@Autowired
	private BatchConfiguration batchConfig;

	@Autowired
	private RediSearchConfiguration rediSearchConfig;

	@Autowired
	private GeneratorConfiguration generatorConfig;

	@Autowired
	private FileConfiguration fileConfig;

	@Autowired
	private RedisWriter redisWriter;

	@Autowired
	private RediSearchWriter rediSearchWriter;

	@Autowired
	private LoadGeneratorStep generatorStep;

	@Autowired
	private LoadFileStep fileStep;

	@Autowired
	private DummyStep dummyStep;

	@Bean
	Job job(JobBuilderFactory jbf, StepBuilderFactory sbf) throws Exception {
		SimpleStepBuilder<Map<String, String>, HashItem> stepBuilder = sbf.get("load-step")
				.<Map<String, String>, HashItem>chunk(batchConfig.getSize());
		if (batchConfig.getMaxThreads() != null) {
			SimpleAsyncTaskExecutor taskExecutor = new SimpleAsyncTaskExecutor();
			taskExecutor.setConcurrencyLimit(batchConfig.getMaxThreads());
			stepBuilder.taskExecutor(taskExecutor);
		}
		StepProvider stepProvider = getStepProvider();
		AbstractItemCountingItemStreamItemReader<Map<String, String>> reader = stepProvider.getReader();
		if (config.getMaxItemCount() != null) {
			reader.setMaxItemCount(config.getMaxItemCount());
		}
		stepBuilder.reader(reader);
		stepBuilder.processor(stepProvider.getProcessor());
		stepBuilder.writer(writer());
		return jbf.get("load").incrementer(new RunIdIncrementer()).start(stepBuilder.build()).build();
	}

	private StepProvider getStepProvider() {
		if (generatorConfig.isEnabled()) {
			return generatorStep;
		}
		if (fileConfig.isEnabled()) {
			return fileStep;
		}
		return dummyStep;
	}

	private ItemWriter<HashItem> writer() {
		if (config.isNoOp()) {
			return new DummyItemWriter();
		}
		if (rediSearchConfig.isEnabled()) {
			CompositeItemWriter<HashItem> writer = new CompositeItemWriter<HashItem>();
			writer.setDelegates(Arrays.asList(redisWriter, rediSearchWriter));
			return writer;
		}
		return redisWriter;
	}

}