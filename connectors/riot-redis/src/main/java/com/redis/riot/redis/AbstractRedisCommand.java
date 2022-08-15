package com.redis.riot.redis;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.riot.AbstractJobCommand;
import com.redis.riot.JobCommandContext;
import com.redis.riot.RedisOptions;

import io.lettuce.core.AbstractRedisClient;
import picocli.CommandLine.Command;

@Command
public abstract class AbstractRedisCommand extends AbstractJobCommand {

	@Override
	protected Job job(JobCommandContext context) throws Exception {
		String name = name();
		RedisCommandTasklet tasklet = new RedisCommandTasklet(context.getRedisClient());
		return job(context, name, tasklet).build();
	}

	private class RedisCommandTasklet implements Tasklet {

		private final AbstractRedisClient redisClient;

		public RedisCommandTasklet(AbstractRedisClient redisClient) {
			this.redisClient = redisClient;
		}

		@Override
		public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
			try (StatefulRedisModulesConnection<String, String> connection = RedisOptions.connect(redisClient)) {
				AbstractRedisCommand.this.execute(connection.sync());
				return RepeatStatus.FINISHED;
			}
		}
	}

	protected abstract void execute(RedisModulesCommands<String, String> commands) throws InterruptedException;

	protected abstract String name();

}
