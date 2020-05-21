package com.redislabs.riot;

import com.redislabs.riot.cli.*;
import org.springframework.batch.item.file.transform.Range;

import com.redislabs.picocliredis.Application;
import com.redislabs.riot.cli.file.RangeConverter;

import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "riot", subcommands = {ImportCommand.class, ExportCommand.class, ReplicateCommand.class, TestCommand.class})
public class Riot extends Application {

    public static void main(String[] args) {
        System.exit(new Riot().execute(args));
    }

    @Override
    protected void registerConverters(CommandLine commandLine) {
        commandLine.registerConverter(Range.class, new RangeConverter());
        super.registerConverters(commandLine);
    }

}
