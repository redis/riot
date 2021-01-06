package com.redislabs.riot.file;

import org.springframework.batch.item.file.transform.Range;

import com.redislabs.riot.RiotApp;

import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "riot-file", subcommands = {KeyValueFileImportCommand.class, DataStructureFileImportCommand.class, FileExportCommand.class})
public class RiotFile extends RiotApp {

    @Override
    protected void registerConverters(CommandLine commandLine) {
        commandLine.registerConverter(Range.class, s -> {
            String[] split = s.split("-");
            return new Range(Integer.parseInt(split[0]), Integer.parseInt(split[1]));
        });
        super.registerConverters(commandLine);
    }

    public static void main(String[] args) {
        System.exit(new RiotFile().execute(args));
    }
}
