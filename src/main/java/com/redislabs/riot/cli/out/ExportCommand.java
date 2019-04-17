package com.redislabs.riot.cli.out;

import com.redislabs.riot.cli.HelpAwareCommand;

import picocli.CommandLine.Command;

@Command(name = "export", description = "Export data from Redis")
public class ExportCommand extends HelpAwareCommand {

}
