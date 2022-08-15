package com.redis.riot;

import java.util.concurrent.Callable;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.SimpleJobBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.util.ClassUtils;

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
		String name = commandSpec == null ? ClassUtils.getShortName(getClass()) : commandSpec.name();
		JobRunner jobRunner = JobRunner.inMemory();
		JobCommandContext context = new JobCommandContext(name, jobRunner, app.getRedisOptions());
		JobExecution execution = jobRunner.run(createJob(context));
		if (execution.getStatus().isUnsuccessful()) {
			return 1;
		}
		return 0;
	}

	protected abstract Job createJob(JobCommandContext context) throws Exception;

	protected SimpleJobBuilder job(JobCommandContext context, String name, Step step) {
		return context.job(name).start(step);
	}

	protected SimpleJobBuilder job(JobCommandContext context, String name, Tasklet tasklet) {
		return job(context, name, context.step(name).tasklet(tasklet).build());
	}

}
