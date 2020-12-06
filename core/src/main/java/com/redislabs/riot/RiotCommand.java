package com.redislabs.riot;

import io.lettuce.core.RedisURI;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

@Slf4j
@Command(abbreviateSynopsis = true, sortOptions = false)
public abstract class RiotCommand extends HelpCommand {

	@ParentCommand
	private RiotApp app;

	@Override
	public void run() {
		try {
			execute(app.getRedisOptions());
		} catch (Exception e) {
			log.error("Could not execute command", e);
		}
	}

	protected abstract void execute(RedisOptions redisOptions) throws Exception;

	protected String toString(RedisURI redisURI) {
		if (redisURI.getSocket() != null) {
			return redisURI.getSocket();
		}
		if (redisURI.getSentinelMasterId() != null) {
			return redisURI.getSentinelMasterId();
		}
		return redisURI.getHost();
	}

}
