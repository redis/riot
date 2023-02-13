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

abstract class AbstractRedisCommand extends AbstractJobCommand {

	@Override
	protected Job job(JobCommandContext context) throws Exception {
		String name = name();
		RedisCommandTasklet tasklet = new RedisCommandTasklet(context);
		return job(context, name, tasklet).build();
	}

	private class RedisCommandTasklet implements Tasklet {

		private final JobCommandContext context;

		public RedisCommandTasklet(JobCommandContext context) {
			this.context = context;
		}

		@Override
		public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
			try (StatefulRedisModulesConnection<String, String> connection = context.connection()) {
				AbstractRedisCommand.this.execute(connection.sync());
				return RepeatStatus.FINISHED;
			}
		}
	}

	protected abstract void execute(RedisModulesCommands<String, String> commands) throws InterruptedException;

	protected abstract String name();

}
