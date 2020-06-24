package com.redislabs.riot.db;

import com.redislabs.riot.RiotApp;
import picocli.CommandLine;

@CommandLine.Command(name = "riot-db", subcommands = {DatabaseImportCommand.class, DatabaseExportCommand.class})
public class App extends RiotApp {

    public static void main(String[] args) {
        System.exit(new App().execute(args));
    }
}
