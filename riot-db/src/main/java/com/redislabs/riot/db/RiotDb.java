package com.redislabs.riot.db;

import com.redislabs.riot.RiotApp;

import picocli.CommandLine.Command;

@Command(name = "riot-db", subcommands = { DatabaseImportCommand.class, DatabaseExportCommand.class })
public class RiotDb extends RiotApp {

    public static void main(String[] args) {
	System.exit(new RiotDb().execute(args));
    }
}
