package com.redis.riot;

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.Callable;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.redis.spring.batch.support.job.JobExecutionWrapper;
import com.redis.spring.batch.support.job.JobFactory;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

@Slf4j
@Data
@EqualsAndHashCode(callSuper = true)
@Command(abbreviateSynopsis = true, sortOptions = false)
public abstract class AbstractRiotCommand extends HelpCommand implements Callable<Integer>, JobExecutionListener {

	@ParentCommand
	private RiotApp app;

	private ExecutionStrategy executionStrategy = ExecutionStrategy.SYNC;

	protected RedisOptions getRedisOptions() {
		return app.getRedisOptions();
	}

	protected final Flow flow(String name, Step... steps) {
		Assert.notEmpty(steps, "Steps are required");
		FlowBuilder<SimpleFlow> flow = new FlowBuilder<>(name);
		Iterator<Step> iterator = Arrays.asList(steps).iterator();
		flow.start(iterator.next());
		while (iterator.hasNext()) {
			flow.next(iterator.next());
		}
		return flow.build();
	}

	@Override
	public Integer call() throws Exception {
		return exitCode(execute());
	}

	private int exitCode(JobExecutionWrapper execution) {
		for (StepExecution stepExecution : execution.getJobExecution().getStepExecutions()) {
			if (stepExecution.getExitStatus().compareTo(ExitStatus.FAILED) >= 0) {
				log.error(stepExecution.getExitStatus().getExitDescription());
				return 1;
			}
		}
		return 0;
	}

	public enum ExecutionStrategy {

		SYNC, ASYNC

	}

	public JobExecutionWrapper execute() throws Exception {
		JobFactory jf = JobFactory.inMemory();
		JobBuilder builder = jf.job(ClassUtils.getShortName(getClass()));
		Job job = builder.listener(this).start(flow(jf)).build().build();
		JobParameters parameters = new JobParameters();
		if (executionStrategy == ExecutionStrategy.SYNC) {
			return jf.run(job, parameters);
		}
		return jf.runAsync(job, parameters).awaitRunning();
	}

	protected abstract Flow flow(JobFactory jobFactory) throws Exception;

	@Override
	public void afterJob(JobExecution jobExecution) {
		getRedisOptions().shutdown();
	}

	@Override
	public void beforeJob(JobExecution jobExecution) {
		// do nothing
	}

}
