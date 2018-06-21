package com.redislabs.recharge;

import java.util.Map;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

import com.redislabs.recharge.config.Recharge;
import com.redislabs.recharge.file.LoadFileStep;
import com.redislabs.recharge.generator.LoadGeneratorStep;
import com.redislabs.recharge.generator.RecordItemReader;
import com.redislabs.recharge.generator.RecordItemWriter;

@Configuration
@EnableBatchProcessing
public class LoadJobConfiguration {

	@Autowired
	private Recharge config;

	@Bean
	Job job(JobBuilderFactory jbf, StepBuilderFactory sbf, LoadFileStep fileStep, LoadGeneratorStep generatorStep)
			throws Exception {
		if (config.getGenerator().getType() != null) {
			config.getRedisearch().setIndex("recordIdx");
			config.getKey().setPrefix("record");
			config.getKey().setFields("id");
			RecordItemReader reader = generatorStep.recordReader();
			RecordItemWriter writer = generatorStep.recordWriter();
			return job(jbf, sbf, reader, writer);
		}
		if (config.getFile().getPath() != null && config.getFile().getPath().length() > 0) {
			FlatFileItemReader<Map<String, String>> reader = fileStep.fileReader();
			ItemWriter<Map<String, String>> writer = fileStep.writer();
			return job(jbf, sbf, reader, writer);
		}
		return null;
	}

	private <T> Job job(JobBuilderFactory jbf, StepBuilderFactory sbf,
			AbstractItemCountingItemStreamItemReader<T> reader, ItemWriter<T> writer) {
		if (config.getMaxItemCount() != null) {
			reader.setMaxItemCount(config.getMaxItemCount());
		}
		SimpleStepBuilder<T, T> stepBuilder = getStepBuilder(sbf, reader, writer);
		if (config.getMaxThreads() != null) {
			SimpleAsyncTaskExecutor taskExecutor = new SimpleAsyncTaskExecutor();
			taskExecutor.setConcurrencyLimit(config.getMaxThreads());
			stepBuilder.taskExecutor(taskExecutor);
		}
		Step step = stepBuilder.build();
		return jbf.get("load").incrementer(new RunIdIncrementer()).start(step).build();
	}

	private <T> SimpleStepBuilder<T, T> getStepBuilder(StepBuilderFactory sbf, ItemReader<T> reader,
			ItemWriter<T> writer) {
		return sbf.get("load-step").<T, T>chunk(config.getBatchSize()).reader(reader).writer(writer);
	}
}