package com.redislabs.riot.cli;

import org.springframework.stereotype.Component;

import com.redislabs.riot.cli.file.DelimitedImportSubCommand;
import com.redislabs.riot.cli.file.FixedLengthImportSubCommand;
import com.redislabs.riot.cli.file.JsonImportSubCommand;

import picocli.CommandLine.Command;

@Component
@Command(name = "import", description = "Import data into Redis", subcommands = { DelimitedImportSubCommand.class,
		FixedLengthImportSubCommand.class, JsonImportSubCommand.class, DatabaseImportSubCommand.class,
		GeneratorImportSubCommand.class }, sortOptions = false)
public class ImportCommand extends AbstractCommand {

}
