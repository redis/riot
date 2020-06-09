package com.redislabs.riot;

import com.redislabs.picocliredis.RedisOptions;
import com.redislabs.riot.cli.*;
import lombok.Getter;
import org.springframework.batch.item.file.transform.Range;

import com.redislabs.picocliredis.Application;
import com.redislabs.riot.cli.file.RangeConverter;

import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "riot", subcommands = {ImportCommand.class, ExportCommand.class, ReplicateCommand.class, TestCommand.class}, sortOptions = false)
public class Riot extends Application {

    @Getter
    @CommandLine.Mixin
    private RedisOptions redisOptions = new RedisOptions();

    public static void main(String[] args) {
        System.exit(new Riot().execute(args));
    }

    @Override
    protected void registerConverters(CommandLine commandLine) {
        commandLine.registerConverter(Range.class, new RangeConverter());
        super.registerConverters(commandLine);
    }

}
