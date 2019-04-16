package com.redislabs.riot.cli;

import org.springframework.stereotype.Component;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Component
@Command(name = "riot", subcommands = { ImportCommand.class, ExportCommand.class })
public class MainCommand extends HelpAwareCommand {

	/**
	 * Just here to avoid picocli complain in Eclipse console
	 */
	@Option(names = "--spring.output.ansi.enabled")
	private String ansiEnabled;

}
