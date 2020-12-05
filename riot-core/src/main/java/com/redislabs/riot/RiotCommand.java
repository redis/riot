package com.redislabs.riot;

import io.lettuce.core.RedisURI;
import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

@Command(abbreviateSynopsis = true, sortOptions = false)
public class RiotCommand extends HelpCommand {

	@ParentCommand
	private RiotApp app;

	protected RiotApp getRiotApp() {
		return app;
	}

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
