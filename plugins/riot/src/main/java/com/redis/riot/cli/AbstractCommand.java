package com.redis.riot.cli;

import com.redis.riot.core.AbstractRedisExecutable;

import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

@Command
public abstract class AbstractCommand extends BaseCommand implements Runnable {

    @ParentCommand
    protected Main parent;

    @Override
    public void run() {
        AbstractRedisExecutable executable = executable();
        executable.setRedisClientOptions(parent.redisArgs.redisClientOptions());
        executable.execute();
    }

    protected abstract AbstractRedisExecutable executable();

}
