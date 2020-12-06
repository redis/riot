package com.redislabs.riot.redis;

import org.springframework.batch.item.redis.support.ClientUtils;

import com.redislabs.riot.RedisOptions;
import com.redislabs.riot.RiotCommand;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.sync.BaseRedisCommands;
import picocli.CommandLine.Command;

@Command
public abstract class AbstractRedisCommand extends RiotCommand {

	@Override
	protected void execute(RedisOptions redisOptions) throws Exception {
		AbstractRedisClient client = null;
		try {
			client = redisOptions.client();
			try (StatefulConnection<String, String> connection = ClientUtils.connection(client)) {
				execute(ClientUtils.sync(client).apply(connection));
			}
		} finally {
			if (client != null) {
				client.shutdown();
			}
		}
	}

	protected abstract void execute(BaseRedisCommands<String, String> commands);

}
