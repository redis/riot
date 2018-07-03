package com.redislabs.recharge;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;

import com.redislabs.recharge.dummy.DummyItemWriter;
import com.redislabs.recharge.dummy.DummyStep;
import com.redislabs.recharge.file.delimited.DelimitedFileStep;
import com.redislabs.recharge.file.fixedlength.FixedLengthFileStep;
import com.redislabs.recharge.generator.GeneratorStep;
import com.redislabs.recharge.redis.HashItem;
import com.redislabs.recharge.redis.RediSearchConfiguration;
import com.redislabs.recharge.redis.RediSearchWriter;
import com.redislabs.recharge.redis.RedisWriter;

@SpringBootApplication
@EnableBatchProcessing
public class RechargeApplication implements ApplicationRunner {
	
	private Logger log = LoggerFactory.getLogger(RechargeApplication.class);

	public static void main(String[] args) {
		SpringApplication.run(RechargeApplication.class, args);
	}

	@Autowired
	private JobLauncher jobLauncher;
	@Autowired
	public JobBuilderFactory jobBuilderFactory;
	@Autowired
	public StepBuilderFactory stepBuilderFactory;
	@Autowired
	private RechargeConfiguration config;
	@Autowired
	private DummyStep dummyStep;
	@Autowired
	private DelimitedFileStep delimitedFileStep;
	@Autowired
	private FixedLengthFileStep fixedLengthFileStep;
	@Autowired
	private GeneratorStep generatorStep;
	@Autowired
	private RediSearchConfiguration rediSearchConfig;
	@Autowired
	private RedisWriter redisWriter;
	@Autowired
	private RediSearchWriter rediSearchWriter;

	private Step loadStep() throws IOException {
		SimpleStepBuilder<Map<String, String>, HashItem> builder = stepBuilderFactory.get("load-step")
				.<Map<String, String>, HashItem>chunk(config.getChunkSize());
		AbstractItemCountingItemStreamItemReader<Map<String, String>> reader = reader();
		if (config.getMaxItemCount() != null) {
			reader.setMaxItemCount(config.getMaxItemCount());
		}
		builder.reader(reader);
		builder.processor(processor());
		builder.writer(writer());
		builder.taskExecutor(taskExecutor());
		builder.throttleLimit(config.getMaxThreads());
		return builder.build();
	}

	private ItemProcessor<Map<String, String>, HashItem> processor() {
		switch (config.getConnector()) {
		case Delimited:
			return delimitedFileStep.processor();
		case FixedLength:
			return fixedLengthFileStep.processor();
		case Generator:
			return generatorStep.processor();
		default:
			return dummyStep.processor();
		}
	}

	private Step unloadStep() throws IOException {
		SimpleStepBuilder<HashItem, Map<String, String>> builder = stepBuilderFactory.get("unload-step")
				.<HashItem, Map<String, String>>chunk(config.getChunkSize());
//		builder.reader(reader());
//		builder.writer(writer());
//		builder.taskExecutor(taskExecutor());
//		builder.throttleLimit(config.getMaxThreads());
		return builder.build();
	}

	private AbstractItemCountingItemStreamItemReader<Map<String, String>> reader() throws IOException {
		switch (config.getConnector()) {
		case Delimited:
			return delimitedFileStep.reader();
		case FixedLength:
			return fixedLengthFileStep.reader();
		case Generator:
			return generatorStep.reader();
		default:
			return dummyStep.reader();
		}
	}

	private TaskExecutor taskExecutor() {
		SimpleAsyncTaskExecutor taskExecutor = new SimpleAsyncTaskExecutor();
		taskExecutor.setConcurrencyLimit(config.getMaxThreads());
		return taskExecutor;
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

	@Override
	public void run(ApplicationArguments args) throws Exception {
		List<String> nonOptionArgs = args.getNonOptionArgs();
		if (nonOptionArgs.size() > 0) {
			switch (nonOptionArgs.get(0)) {
			case "load":
				launchJob("load-job", loadStep());
				break;
			case "unload":
				launchJob("unload-job", unloadStep());
				break;
			default:
				break;
			}
		} else {
			log.error("No command given. Run 'recharge help' for usage");
		}
	}

	private void launchJob(String jobName, Step step) throws NoSuchJobException, JobExecutionAlreadyRunningException,
			JobRestartException, JobInstanceAlreadyCompleteException, JobParametersInvalidException, IOException {
		Job job = jobBuilderFactory.get(jobName).incrementer(new RunIdIncrementer()).flow(step).end().build();
		jobLauncher.run(job, new JobParameters());
	}

}
