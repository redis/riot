package com.redis.riot.cli.common;

import java.time.Duration;
import java.util.concurrent.Callable;

import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;

import com.redis.riot.cli.Main;
import com.redis.riot.core.ThrottledItemReader;
import com.redis.spring.batch.common.JobRunner;
import com.redis.spring.batch.common.StepOptions;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.ParentCommand;
import picocli.CommandLine.Spec;

@Command(usageHelpAutoWidth = true)
public abstract class AbstractCommand implements Callable<Integer> {

	@ParentCommand
	private Main riot;

	@Spec
	private CommandSpec commandSpec;

	@Mixin
	private HelpOptions helpOptions;

	@Mixin
	private TransferOptions transferOptions = new TransferOptions();

	protected String commandName() {
		return commandSpec.qualifiedName("-");
	}

	protected RedisOptions getRedisOptions() {
		return riot.getRedisOptions();
	}

	public void setRiot(Main riot) {
		this.riot = riot;
	}

	public void setCommandSpec(CommandSpec commandSpec) {
		this.commandSpec = commandSpec;
	}

	@Override
	public Integer call() throws Exception {
		JobRunner jobRunner = JobRunner.inMemory();
		try (CommandContext context = context(jobRunner, getRedisOptions())) {
			Job job = job(context);
			JobExecution execution = jobRunner.run(job);
			jobRunner.awaitTermination(execution);
			if (execution.getStatus().isUnsuccessful()) {
				return 1;
			}
			return 0;
		}
	}

	protected CommandContext context(JobRunner jobRunner, RedisOptions redisOptions) {
		return new CommandContext(jobRunner, redisOptions);
	}

	protected abstract Job job(CommandContext context);

	public TransferOptions getTransferOptions() {
		return transferOptions;
	}

	public void setTransferOptions(TransferOptions options) {
		this.transferOptions = options;
	}

	protected <I, O> SimpleStepBuilder<I, O> step(CommandContext context, ItemReader<I> reader, ItemWriter<O> writer) {
		return step(context, reader, null, writer);
	}

	protected <I, O> SimpleStepBuilder<I, O> step(CommandContext context, ItemReader<I> reader,
			ItemProcessor<I, O> processor, ItemWriter<O> writer) {
		return step(context, commandName(), reader, processor, writer);
	}

	protected <I, O> SimpleStepBuilder<I, O> step(CommandContext context, String name, ItemReader<I> reader,
			ItemWriter<O> writer) {
		return step(context, name, reader, null, writer);
	}

	protected <I, O> SimpleStepBuilder<I, O> step(CommandContext context, String name, ItemReader<I> reader,
			ItemProcessor<I, O> processor, ItemWriter<O> writer) {
		return step(context, name, reader, processor, writer, stepOptions());
	}

	protected <I, O> SimpleStepBuilder<I, O> step(CommandContext context, String name, ItemReader<I> reader,
			ItemProcessor<I, O> processor, ItemWriter<O> writer, StepOptions stepOptions) {
		return context.getJobRunner().step(name, throttle(reader), processor, writer, stepOptions);
	}

	protected StepOptions stepOptions() {
		return transferOptions.stepOptions();
	}

	private <I> ItemReader<I> throttle(ItemReader<I> reader) {
		Duration sleep = Duration.ofMillis(transferOptions.getSleep());
		if (sleep.isNegative() || sleep.isZero()) {
			return reader;
		}
		return new ThrottledItemReader<>(reader, sleep);
	}

	protected ProgressMonitor.Builder progressMonitor() {
		return ProgressMonitor.style(transferOptions.getProgressBarStyle())
				.updateInterval(Duration.ofMillis(transferOptions.getProgressUpdateInterval()));
	}

	protected <I, O> SimpleStepBuilder<I, O> step(SimpleStepBuilder<I, O> step, ProgressMonitor monitor) {
		if (transferOptions.isProgressEnabled()) {
			step.listener((StepExecutionListener) monitor);
			step.listener((ItemWriteListener<Object>) monitor);
		}
		return step;
	}

}
