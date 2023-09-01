package com.redis.riot.cli;

import org.slf4j.event.Level;

import com.redis.riot.core.Executable;

import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

@Command
abstract class AbstractCommand<C extends IO> extends BaseCommand implements Runnable {

    protected static final String SLF4J_LOGGER = "org.slf4j.simpleLogger.";

    @ParentCommand
    protected C parent;

    @Override
    public void run() {
        setup();
        getExecutable().execute();
    }

    protected void setup() {
        setLogLevel("defaultLogLevel", Level.ERROR);
    }

    protected void setLogLevel(String logger, Level level) {
        if (level == null) {
            return;
        }
        System.setProperty(SLF4J_LOGGER + logger, level.name().toLowerCase());
    }

    protected abstract Executable getExecutable();

}
