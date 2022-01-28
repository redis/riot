package com.redis.riot;

import java.time.Duration;
import java.util.concurrent.Callable;

import org.awaitility.Awaitility;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.util.ClassUtils;

import com.redis.spring.batch.support.JobRepositoryBuilder;
import com.redis.spring.batch.support.JobRunner;

import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.ParentCommand;
import picocli.CommandLine.Spec;

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

	@Override
	public Integer call() throws Exception {
		return exitCode(execute());
	}

	private int exitCode(JobExecution execution) {
		if (execution.getStatus().isUnsuccessful()) {
			return 1;
		}
		return 0;
	}

	public JobExecution execute() throws Exception {
		JobRunner runner = getJobRunner();
		return getJobRunner().run(job(configureJob(runner.job(commandName()))));
	}

	protected abstract Job job(JobBuilder jobBuilder) throws Exception;

	protected JobBuilder configureJob(JobBuilder job) {
		return job.listener(new JobExecutionListenerSupport() {
			@Override
			public void afterJob(JobExecution jobExecution) {
				getRedisOptions().shutdown();
			}
		});
	}

	private String commandName() {
		if (commandSpec == null) {
			return ClassUtils.getShortName(getClass());
		}
		return commandSpec.name();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected <B extends JobRepositoryBuilder> B configureJobRepository(B builder) throws Exception {
		JobRunner runner = getJobRunner();
		return (B) builder.jobRepository(runner.getJobRepository()).transactionManager(runner.getTransactionManager());
	}

}
