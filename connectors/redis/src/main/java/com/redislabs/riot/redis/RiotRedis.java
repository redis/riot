package com.redislabs.riot.redis;

import com.redislabs.riot.RiotApp;
import picocli.CommandLine;

@CommandLine.Command(name = "riot-redis", subcommands = {ReplicateCommand.class, InfoCommand.class, LatencyCommand.class, PingCommand.class})
public class RiotRedis extends RiotApp {

    public static void main(String[] args) {
        System.exit(new RiotRedis().execute(args));
    }
}
