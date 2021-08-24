package com.redis.riot.file;

import com.redis.riot.RiotApp;
import picocli.CommandLine.Command;

@Command(name = "riot-file", subcommands = {FileImportCommand.class, DumpFileImportCommand.class, FileExportCommand.class})
public class RiotFile extends RiotApp {

    public static void main(String[] args) {
        System.exit(new RiotFile().execute(args));
    }

}
