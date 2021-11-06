package com.redis.riot.redis;

import com.redis.riot.RiotApp;

import picocli.CommandLine.Command;

@Command(name = "riot-redis", subcommands = { ReplicateCommand.class, CompareCommand.class, InfoCommand.class,
		LatencyCommand.class, PingCommand.class })
public class RiotRedis extends RiotApp {

	public static void main(String[] args) {
		System.exit(new RiotRedis().execute(args));
	}

}
