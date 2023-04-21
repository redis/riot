package com.redis.riot.cli;

import java.util.concurrent.Callable;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;

import com.redis.spring.batch.common.JobRunner;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.ParentCommand;

@Command(usageHelpAutoWidth = true)
public abstract class AbstractJobCommand implements Callable<Integer> {

	@ParentCommand
	private Main app;
	@Mixin
	private HelpOptions helpOptions;

	public void setApp(Main app) {
		this.app = app;
	}

	@Override
	public Integer call() throws Exception {
		JobRunner jobRunner = JobRunner.inMemory();
		try (JobCommandContext context = context(jobRunner, app.getRedisOptions())) {
			JobExecution execution = jobRunner.getJobLauncher().run(job(context), new JobParameters());
			jobRunner.awaitTermination(execution);
			if (execution.getStatus().isUnsuccessful()) {
				return 1;
			}
			return 0;
		}
	}

	protected JobCommandContext context(JobRunner jobRunner, RedisOptions redisOptions) {
		return new JobCommandContext(jobRunner, redisOptions);
	}

	protected abstract Job job(JobCommandContext context);

}
