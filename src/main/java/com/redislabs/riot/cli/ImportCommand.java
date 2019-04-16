package com.redislabs.riot.cli;

import org.springframework.stereotype.Component;

import lombok.Getter;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

@Component
@Command(name = "import", description = "Import data into Redis", subcommands = { DelimitedImportSubCommand.class,
		FixedLengthImportSubCommand.class, JsonImportSubCommand.class,
		DatabaseImportSubCommand.class, GeneratorImportSubCommand.class }, sortOptions = false)
public class ImportCommand extends HelpAwareCommand {

	@Mixin
	@Getter
	private RedisConnectionOptions redis;

}
