package com.redis.riot;

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.Callable;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.redis.spring.batch.builder.JobRepositoryBuilder;
import com.redis.spring.batch.support.JobRunner;

import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.ParentCommand;
import picocli.CommandLine.Spec;

@Slf4j
@Command(abbreviateSynopsis = true, sortOptions = false)
public abstract class AbstractRiotCommand extends HelpCommand implements Callable<Integer> {

	@Spec
	private CommandSpec commandSpec;
	@ParentCommand
	private RiotApp app;

	public void setApp(RiotApp app) {
		this.app = app;
	}

	private JobRunner jobRunner;

	protected RedisOptions getRedisOptions() {
		return app.getRedisOptions();
	}

	protected JobRunner getJobRunner() throws Exception {
		if (jobRunner == null) {
			@SuppressWarnings("deprecation")
			org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean bean = new org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean();
			bean.afterPropertiesSet();
			this.jobRunner = new JobRunner(bean.getObject(), bean.getTransactionManager());
		}
		return jobRunner;
	}

	protected StepBuilder step(String name) throws Exception {
		return getJobRunner().step(name);
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

	private int exitCode(JobExecution execution) {
		for (StepExecution stepExecution : execution.getStepExecutions()) {
			if (stepExecution.getExitStatus().compareTo(ExitStatus.FAILED) >= 0) {
				log.error(stepExecution.getExitStatus().getExitDescription());
				return 1;
			}
		}
		return 0;
	}

	public JobExecution execute() throws Exception {
		JobRunner runner = getJobRunner();
		return runner.run(configureJob(runner.job(commandName())).start(flow()).build().build());
	}

	protected JobBuilder configureJob(JobBuilder job) {
		return job.listener(new CleanupJobExecutionListener(getRedisOptions()));
	}

	private String commandName() {
		if (commandSpec == null) {
			return ClassUtils.getShortName(getClass());
		}
		return commandSpec.name();
	}

	protected static class CleanupJobExecutionListener implements JobExecutionListener {

		private final RedisOptions redisOptions;

		public CleanupJobExecutionListener(RedisOptions redisOptions) {
			this.redisOptions = redisOptions;
		}

		@Override
		public void beforeJob(JobExecution jobExecution) {
			// do nothing
		}

		@Override
		public void afterJob(JobExecution jobExecution) {
			redisOptions.shutdown();
		}

	}

	protected abstract Flow flow() throws Exception;

	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected <B extends JobRepositoryBuilder> B configureJobRepository(B builder) throws Exception {
		JobRunner runner = getJobRunner();
		return (B) builder.jobRepository(runner.getJobRepository()).transactionManager(runner.getTransactionManager());
	}

}
