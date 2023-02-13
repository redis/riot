package com.redis.riot;

import java.util.concurrent.Callable;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.SimpleJobBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;

import com.redis.spring.batch.common.JobRunner;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.ParentCommand;
import picocli.CommandLine.Spec;

@Command(usageHelpAutoWidth = true)
public abstract class AbstractJobCommand implements Callable<Integer> {

	@Spec
	protected CommandSpec commandSpec;
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
			JobExecution execution = jobRunner.run(job(context));
			if (execution.getStatus().isUnsuccessful()) {
				return 1;
			}
			return 0;
		}
	}

	protected JobCommandContext context(JobRunner jobRunner, RedisOptions redisOptions) {
		return new JobCommandContext(jobRunner, redisOptions);
	}

	protected abstract Job job(JobCommandContext context) throws Exception;

	protected SimpleJobBuilder job(JobCommandContext context, String name, Step step) {
		return context.job(name).start(step);
	}

	protected SimpleJobBuilder job(JobCommandContext context, String name, Tasklet tasklet) {
		return job(context, name, context.step(name).tasklet(tasklet).build());
	}

}
