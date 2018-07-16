package com.redislabs.recharge;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redislabs.recharge.RechargeConfiguration.DataType;
import com.redislabs.recharge.dummy.DummyStep;
import com.redislabs.recharge.file.DelimitedFileStep;
import com.redislabs.recharge.file.FixedLengthFileStep;
import com.redislabs.recharge.generator.GeneratorStep;
import com.redislabs.recharge.redis.GeoWriter;
import com.redislabs.recharge.redis.HashWriter;
import com.redislabs.recharge.redis.ListWriter;
import com.redislabs.recharge.redis.NilWriter;
import com.redislabs.recharge.redis.RediSearchWriter;
import com.redislabs.recharge.redis.SetWriter;
import com.redislabs.recharge.redis.StringWriter;
import com.redislabs.recharge.redis.ZSetWriter;

import lombok.extern.slf4j.Slf4j;

@SpringBootApplication
@EnableBatchProcessing
@Slf4j
public class RechargeApplication implements ApplicationRunner {

	public static void main(String[] args) {
		ConfigurableApplicationContext context = SpringApplication.run(RechargeApplication.class, args);
		SpringApplication.exit(context);
	}

	@Autowired
	private JobLauncher jobLauncher;
	@Autowired
	private JobBuilderFactory jobBuilderFactory;
	@Autowired
	private StepBuilderFactory stepBuilderFactory;
	@Autowired
	private RechargeConfiguration config;
	@Autowired
	private RedisProperties redisConfig;
	@Autowired
	private DummyStep dummyStep;
	@Autowired
	private DelimitedFileStep delimitedFileStep;
	@Autowired
	private FixedLengthFileStep fixedLengthFileStep;
	@Autowired
	private GeneratorStep generatorStep;
	@Autowired
	private StringRedisTemplate redisTemplate;

	private Step loadStep() throws IOException {
		SimpleStepBuilder<Map<String, Object>, Map<String, Object>> builder = stepBuilderFactory.get("load-step")
				.<Map<String, Object>, Map<String, Object>>chunk(config.getChunkSize());
		AbstractItemCountingItemStreamItemReader<Map<String, Object>> reader = reader();
		if (config.getMaxItemCount() != -1) {
			reader.setMaxItemCount(config.getMaxItemCount());
		}
		builder.reader(reader);
		builder.writer(writer());
		builder.taskExecutor(taskExecutor());
		builder.throttleLimit(config.getMaxThreads());
		return builder.build();
	}

	private Step unloadStep() throws IOException {
		SimpleStepBuilder<Map<String, Object>, Map<String, Object>> builder = stepBuilderFactory.get("unload-step")
				.<Map<String, Object>, Map<String, Object>>chunk(config.getChunkSize());
//		builder.reader(reader());
//		builder.writer(writer());
//		builder.taskExecutor(taskExecutor());
//		builder.throttleLimit(config.getMaxThreads());
		return builder.build();
	}

	private AbstractItemCountingItemStreamItemReader<Map<String, Object>> reader() throws IOException {
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

	private ItemWriter<Map<String, Object>> writer() {
		List<DataType> dataTypes = config.getDatatypes();
		if (dataTypes.isEmpty()) {
			dataTypes.add(DataType.Hash);
		}
		if (dataTypes.size() == 1) {
			return getWriter(dataTypes.get(0));
		}
		CompositeItemWriter<Map<String, Object>> writer = new CompositeItemWriter<Map<String, Object>>();
		List<ItemWriter<? super Map<String, Object>>> delegates = new ArrayList<>();
		for (DataType dataType : dataTypes) {
			delegates.add(getWriter(dataType));
		}
		writer.setDelegates(delegates);
		return writer;
	}

	private ItemWriter<Map<String, Object>> getWriter(DataType dataType) {
		switch (dataType) {
		case Nil:
			return new NilWriter();
		case String:
			return new StringWriter(getObjectWriter(), config.getKey(), redisTemplate);
		case List:
			return new ListWriter(config.getKey(), redisTemplate, config.getList());
		case Set:
			return new SetWriter(config.getKey(), redisTemplate, config.getSet());
		case ZSet:
			return new ZSetWriter(config.getKey(), redisTemplate, config.getZset());
		case Geo:
			return new GeoWriter(config.getKey(), redisTemplate, config.getGeo());
		case RediSearchIndex:
			return new RediSearchWriter(config.getKey(), config.getRedisearch(), redisConfig);
		default:
			return new HashWriter(config.getKey(), redisTemplate);
		}
	}

	private ObjectWriter getObjectWriter() {
		switch (config.getString().getFormat()) {
		case Xml:
			return new XmlMapper().writer().withRootName(config.getXml().getRootName());
		default:
			return new ObjectMapper().writer();
		}
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
