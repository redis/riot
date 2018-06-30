package com.redislabs.recharge;

import java.util.Arrays;
import java.util.Map;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

import com.redislabs.recharge.config.BatchConfiguration;
import com.redislabs.recharge.config.RechargeConfiguration;
import com.redislabs.recharge.file.FileConfiguration;
import com.redislabs.recharge.file.LoadFileStep;
import com.redislabs.recharge.generator.GeneratorConfiguration;
import com.redislabs.recharge.generator.LoadGeneratorStep;
import com.redislabs.recharge.noop.NoOpWriter;
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

	@Bean
	Job job(JobBuilderFactory jbf, StepBuilderFactory sbf, LoadFileStep fileStep, LoadGeneratorStep generatorStep)
			throws Exception {
		if (generatorConfig.isEnabled()) {
			return job(jbf, sbf, generatorStep.reader(), generatorStep.processor());
		}
		if (fileConfig.isEnabled()) {
			return job(jbf, sbf, fileStep.reader(), fileStep.processor());
		}
		return null;
	}

	private ItemWriter<HashItem> writer() {
		if (config.isNoOp()) {
			return new NoOpWriter();
		}
		if (rediSearchConfig.isEnabled()) {
			CompositeItemWriter<HashItem> writer = new CompositeItemWriter<HashItem>();
			writer.setDelegates(Arrays.asList(redisWriter, rediSearchWriter));
			return writer;
		}
		return redisWriter;
	}

	private Job job(JobBuilderFactory jbf, StepBuilderFactory sbf,
			AbstractItemCountingItemStreamItemReader<Map<String, String>> reader,
			ItemProcessor<Map<String, String>, HashItem> processor) {
		if (config.getMaxItemCount() != null) {
			reader.setMaxItemCount(config.getMaxItemCount());
		}
		SimpleStepBuilder<Map<String, String>, HashItem> stepBuilder = sbf.get("load-step")
				.<Map<String, String>, HashItem>chunk(batchConfig.getChunkSize()).reader(reader).processor(processor)
				.writer(writer());
		if (batchConfig.getMaxThreads() != null) {
			SimpleAsyncTaskExecutor taskExecutor = new SimpleAsyncTaskExecutor();
			taskExecutor.setConcurrencyLimit(batchConfig.getMaxThreads());
			stepBuilder.taskExecutor(taskExecutor);
		}
		Step step = stepBuilder.build();
		return jbf.get("load").incrementer(new RunIdIncrementer()).start(step).build();
	}

}