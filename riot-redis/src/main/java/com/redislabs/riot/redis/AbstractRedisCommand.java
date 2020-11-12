package com.redislabs.riot.redis;

import org.springframework.batch.item.redis.support.RedisConnectionBuilder;

import com.redislabs.riot.RiotCommand;

import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.sync.BaseRedisCommands;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine.Command;

@Slf4j
@Command
public abstract class AbstractRedisCommand extends RiotCommand {

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public void run() {
	RedisConnectionBuilder<?> builder;
	try {
	    builder = (RedisConnectionBuilder) configure(new RedisConnectionBuilder());
	} catch (Exception e) {
	    log.error("Could not connect to Redis", e);
	    return;
	}
	StatefulConnection<String, String> connection = builder.connection();
	BaseRedisCommands<String, String> commands = builder.sync().apply(connection);
	execute(commands);
    }

    protected abstract void execute(BaseRedisCommands<String, String> commands);

}
