package com.redis.riot.cli;

import com.redis.riot.core.AbstractRiotRunnable;

import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

@Command
public abstract class AbstractCommand extends BaseCommand implements Runnable {

    @ParentCommand
    protected Main parent;

    @Override
    public void run() {
        AbstractRiotRunnable executable = executable();
        executable.setRedisOptions(parent.redisArgs.redisClientOptions());
        executable.run();
    }

    protected abstract AbstractRiotRunnable executable();

}
