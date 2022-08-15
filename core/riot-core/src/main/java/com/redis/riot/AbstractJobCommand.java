package com.redis.riot;

import java.util.concurrent.Callable;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.SimpleJobBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;

import com.redis.spring.batch.support.JobRunner;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.ParentCommand;
import picocli.CommandLine.Spec;

@Command
public abstract class AbstractJobCommand implements Callable<Integer> {

	@Spec
	private CommandSpec commandSpec;
	@ParentCommand
	private RiotApp app;
	@Mixin
	private HelpOptions helpOptions;

	public void setApp(RiotApp app) {
		this.app = app;
	}

	@Override
	public Integer call() throws Exception {
		JobRunner jobRunner = JobRunner.inMemory();
		JobExecution execution = jobRunner.run(job(context(jobRunner, app.getRedisOptions())));
		if (execution.getStatus().isUnsuccessful()) {
			return 1;
		}
		return 0;
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
