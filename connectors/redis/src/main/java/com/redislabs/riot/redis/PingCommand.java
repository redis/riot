package com.redislabs.riot.redis;

import io.lettuce.core.api.sync.BaseRedisCommands;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine;

@Slf4j
@CommandLine.Command(name = "ping", aliases = {"p"}, description = "Execute PING command")
public class PingCommand extends AbstractRedisCommand {

    @Override
    protected void execute(BaseRedisCommands<String, String> commands) {
        log.info("Received ping reply: {}", commands.ping());
    }

}
