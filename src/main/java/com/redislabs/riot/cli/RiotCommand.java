package com.redislabs.riot.cli;

import com.redislabs.picocliredis.Application;
import com.redislabs.picocliredis.HelpCommand;
import com.redislabs.picocliredis.RedisCommandLineOptions;
import com.redislabs.riot.Riot;
import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

@Command(sortOptions = false)
public abstract class RiotCommand extends HelpCommand implements Runnable {

    @ParentCommand
    private Riot parent = new Riot();

    public RedisCommandLineOptions getOptions() {
        return parent.getRedis();
    }

    protected boolean isQuiet() {
        return parent.getDebugLevel() == Application.LogLevel.QUIET;
    }

}
