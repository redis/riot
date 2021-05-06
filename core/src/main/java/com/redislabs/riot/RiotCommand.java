package com.redislabs.riot;

import io.lettuce.core.RedisURI;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

import java.util.concurrent.Callable;

@Slf4j
@Data
@EqualsAndHashCode(callSuper = true)
@Command(abbreviateSynopsis = true, sortOptions = false)
public abstract class RiotCommand extends HelpCommand implements Callable<Integer> {

    @SuppressWarnings("unused")
    @ParentCommand
    private RiotApp app;

    protected RedisOptions getRedisOptions() {
        return app.getRedisOptions();
    }

    protected String name(RedisURI redisURI) {
        if (redisURI.getSocket() != null) {
            return redisURI.getSocket();
        }
        if (redisURI.getSentinelMasterId() != null) {
            return redisURI.getSentinelMasterId();
        }
        return redisURI.getHost();
    }

}
