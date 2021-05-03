package com.redislabs.riot.redis;

import com.redislabs.riot.RiotApp;

import picocli.CommandLine.Command;

@Command(name = "riot-redis", subcommands = {KeyDumpReplicateCommand.class, DataStructureReplicateCommand.class, InfoCommand.class, LatencyCommand.class, PingCommand.class})
public class RiotRedis extends RiotApp {

    public static void main(String[] args) {
        System.exit(new RiotRedis().execute(args));
    }

}
