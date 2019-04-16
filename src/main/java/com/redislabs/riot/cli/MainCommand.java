package com.redislabs.riot.cli;

import org.springframework.stereotype.Component;

import picocli.CommandLine.Command;

@Component
@Command(name = "riot", subcommands = { ImportCommand.class, ExportCommand.class })
public class MainCommand extends HelpAwareCommand {

}
