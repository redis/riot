package com.redis.riot.cli;

import com.redis.riot.core.Executable;

import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

@Command
public abstract class AbstractCommand extends BaseCommand implements Runnable {

    @ParentCommand
    protected Main parent;

    @Override
    public void run() {
        getExecutable().execute();
    }

    protected abstract Executable getExecutable();

}
