package com.redislabs.riot.file;

import com.redislabs.riot.RiotApp;
import picocli.CommandLine.Command;

@Command(name = "riot-file", subcommands = {FileImportCommand.class, DumpFileImportCommand.class, FileExportCommand.class})
public class RiotFile extends RiotApp {

    public static void main(String[] args) {
        System.exit(new RiotFile().execute(args));
    }

}
