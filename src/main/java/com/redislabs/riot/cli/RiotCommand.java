package com.redislabs.riot.cli;

import com.redislabs.lettuce.helper.RedisOptions;
import com.redislabs.picocliredis.HelpCommand;
import com.redislabs.riot.Riot;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

@Command
public class RiotCommand extends HelpCommand implements Runnable {

    @ParentCommand
    private Riot riot;

    protected boolean isQuiet() {
        return riot.isQuiet();
    }

    @Override
    public void run() {
        CommandLine.usage(this, System.out);
    }

    public RedisOptions redisOptions() {
        return riot.redisOptions();
    }


}
