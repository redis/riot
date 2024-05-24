package com.redis.riot;

import org.springframework.batch.core.Job;

import com.redis.riot.core.Step;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(name = "ping", description = "Test connectivity to a Redis server.")
public class Ping extends AbstractRedisCommand {

	@ArgGroup(exclusive = false)
	private PingArgs pingArgs = new PingArgs();

	public void copyTo(Ping target) {
		super.copyTo(target);
		target.pingArgs = pingArgs;
	}

	@Override
	protected Job job() {
		PingExecutionItemReader reader = new PingExecutionItemReader(redisCommands);
		reader.setMaxItemCount(pingArgs.getCount());
		PingLatencyItemWriter writer = new PingLatencyItemWriter(parent.getOut());
		writer.setLatencyArgs(pingArgs.getLatencyArgs());
		Step<PingExecution, PingExecution> step = new Step<>(reader, writer);
		step.taskName("Pinging");
		step.maxItemCount(pingArgs.getCount());
		return job(step);
	}

	public PingArgs getPingArgs() {
		return pingArgs;
	}

	public void setPingArgs(PingArgs pingArgs) {
		this.pingArgs = pingArgs;
	}

}
