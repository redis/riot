package com.redislabs.riot.cli;

import org.springframework.stereotype.Component;

import picocli.CommandLine.Command;

@Component
@Command(name = "export", description = "Export from Redis", subcommands = {})
public class ExportCommand {

}
