package com.redislabs.recharge.batch;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.builder.JobFlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.listener.StepExecutionListenerSupport;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.stereotype.Component;

import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.recharge.RechargeConfiguration;
import com.redislabs.recharge.RechargeConfiguration.FlowConfiguration;
import com.redislabs.recharge.RechargeConfiguration.NilConfiguration;
import com.redislabs.recharge.RechargeConfiguration.ReaderConfiguration;
import com.redislabs.recharge.RechargeConfiguration.RedisType;
import com.redislabs.recharge.RechargeConfiguration.RedisWriterConfiguration;
import com.redislabs.recharge.RechargeConfiguration.WriterConfiguration;
import com.redislabs.recharge.file.FileBatchConfiguration;
import com.redislabs.recharge.generator.GeneratorEntityItemReader;
import com.redislabs.recharge.redis.AbstractRedisWriter;
import com.redislabs.recharge.redis.FTAddWriter;
import com.redislabs.recharge.redis.GeoAddWriter;
import com.redislabs.recharge.redis.HIncrByWriter;
import com.redislabs.recharge.redis.HMSetWriter;
import com.redislabs.recharge.redis.LPushWriter;
import com.redislabs.recharge.redis.NilWriter;
import com.redislabs.recharge.redis.SAddWriter;
import com.redislabs.recharge.redis.StringWriter;
import com.redislabs.recharge.redis.SuggestionWriter;
import com.redislabs.recharge.redis.ZAddWriter;

import lombok.extern.slf4j.Slf4j;

@Component
@EnableBatchProcessing
@Slf4j
public class BatchConfiguration {

	@Autowired
	private JobBuilderFactory jobBuilderFactory;
	@Autowired
	private StepBuilderFactory stepBuilderFactory;
	@Autowired
	private RechargeConfiguration rechargeConfig;
	@Autowired
	private FileBatchConfiguration fileBatchConfig;
	@Autowired
	private RediSearchClient client;
	@Autowired
	private JobLauncher jobLauncher;

	private List<Flow> getFlows() throws Exception {
		if (rechargeConfig.isFlushall()) {
			StatefulRediSearchConnection<String, String> connection = client.connect();
			log.warn("***** FLUSHALL in {} millis *****", rechargeConfig.getFlushallWait());
			Thread.sleep(rechargeConfig.getFlushallWait());
			try {
				connection.sync().flushall();
			} finally {
				connection.close();
			}
		}
		List<Flow> flows = new ArrayList<>();
		for (FlowConfiguration flow : rechargeConfig.getFlows()) {
			StatefulRediSearchConnection<String, String> connection = client.connect();
			int flowPosition = rechargeConfig.getFlows().indexOf(flow) + 1;
			String flowName = "flow" + flowPosition;
			String stepName = flowName + "-step";
			SimpleStepBuilder<Map<String, Object>, Map<String, Object>> builder = stepBuilderFactory.get(stepName)
					.<Map<String, Object>, Map<String, Object>>chunk(flow.getChunkSize());
			AbstractItemCountingItemStreamItemReader<Map<String, Object>> reader = getReader(flow.getReader());
			if (flow.getMaxItemCount() > 0) {
				reader.setMaxItemCount(flow.getMaxItemCount());
			}
			reader.setName(flowName + "-reader");
			builder.reader(reader);
			if (flow.getProcessor() != null) {
				builder.processor(new SpelProcessor(connection, flow.getProcessor()));
			}
			builder.writer(getWriter(flow.getWriters()));
			builder.listener(new StepExecutionListenerSupport() {

				@Override
				public ExitStatus afterStep(StepExecution stepExecution) {
					if (connection != null) {
						connection.close();
					}
					return null;
				}
			});
			flows.add(new FlowBuilder<Flow>(flowName).from(builder.build()).end());
		}
		return flows;
	}

	private ItemWriter<? super Map<String, Object>> getWriter(List<WriterConfiguration> writers) {
		if (writers.size() == 0) {
			RedisWriterConfiguration config = new RedisWriterConfiguration();
			config.setNil(new NilConfiguration());
			return new NilWriter(client, config);
		}
		if (writers.size() == 1) {
			return getRedisWriter(writers.get(0));
		}
		CompositeItemWriter<Map<String, Object>> composite = new CompositeItemWriter<>();
		composite.setDelegates(writers.stream().map(writer -> getRedisWriter(writer)).collect(Collectors.toList()));
		return composite;
	}

	private AbstractRedisWriter getRedisWriter(WriterConfiguration writer) {
		RedisWriterConfiguration redis = writer.getRedis();
		switch (getRedisType(redis)) {
		case nil:
			return new NilWriter(client, redis);
		case string:
			return new StringWriter(client, redis);
		case geo:
			return new GeoAddWriter(client, redis);
		case list:
			return new LPushWriter(client, redis);
		case search:
			return new FTAddWriter(client, redis);
		case suggest:
			return new SuggestionWriter(client, redis);
		case zset:
			return new ZAddWriter(client, redis);
		case set:
			return new SAddWriter(client, redis);
		default:
			if (redis.getHash() != null && redis.getHash().getIncrby() != null) {
				return new HIncrByWriter(client, redis);
			}
			return new HMSetWriter(client, redis);
		}
	}

	private RedisType getRedisType(RedisWriterConfiguration redis) {
		if (redis.getType() == null) {
			if (redis.getSet() != null) {
				return RedisType.set;
			}
			if (redis.getGeo() != null) {
				return RedisType.geo;
			}
			if (redis.getList() != null) {
				return RedisType.list;
			}
			if (redis.getNil() != null) {
				return RedisType.nil;
			}
			if (redis.getSearch() != null) {
				return RedisType.search;
			}
			if (redis.getSet() != null) {
				return RedisType.set;
			}
			if (redis.getString() != null) {
				return RedisType.string;
			}
			if (redis.getSuggest() != null) {
				return RedisType.suggest;
			}
			if (redis.getZset() != null) {
				return RedisType.zset;
			}
			return RedisType.hash;
		}
		return redis.getType();
	}

	private Job getJob() throws Exception {
		List<Flow> flows = getFlows();
		JobBuilder builder = jobBuilderFactory.get("recharge-job");
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

	private AbstractItemCountingItemStreamItemReader<Map<String, Object>> getReader(ReaderConfiguration reader)
			throws Exception {
		if (reader.getFile() != null) {
			return fileBatchConfig.getReader(reader.getFile());
		}
		if (reader.getGenerator() != null) {
			Map<String, String> generatorFields = reader.getGenerator().getFields();
			return new GeneratorEntityItemReader(client, reader.getGenerator().getLocale(), generatorFields);
		}
		return null;
	}

	public void runJob() throws JobExecutionAlreadyRunningException, JobRestartException,
			JobInstanceAlreadyCompleteException, JobParametersInvalidException, Exception {
		jobLauncher.run(getJob(), new JobParameters());

	}

}
