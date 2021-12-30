package com.redis.riot.redis;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.repeat.RepeatStatus;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.riot.AbstractRiotCommand;

import picocli.CommandLine.Command;

@Command
public abstract class AbstractRedisCommandCommand extends AbstractRiotCommand {

	@Override
	protected Job job(JobBuilder jobBuilder) throws Exception {
		String name = name();
		Step step = step(name).tasklet((contribution, chunkContext) -> {
			try (StatefulRedisModulesConnection<String, String> connection = getRedisOptions().connect()) {
				execute(connection.sync());
				return RepeatStatus.FINISHED;
			}
		}).build();
		return jobBuilder.start(step).build();
	}

	protected abstract void execute(RedisModulesCommands<String, String> commands) throws InterruptedException;

	protected abstract String name();

}
