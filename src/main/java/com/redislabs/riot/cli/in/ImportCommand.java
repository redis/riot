package com.redislabs.riot.cli.in;

import java.util.Map;

import org.springframework.stereotype.Component;

import com.redislabs.riot.cli.AbstractCommand;
import com.redislabs.riot.cli.file.DelimitedImportSubCommand;
import com.redislabs.riot.cli.file.FixedLengthImportSubCommand;
import com.redislabs.riot.cli.file.JsonImportSubCommand;

import picocli.CommandLine.Command;

@Component
@Command(name = "import", description = "Import into Redis", subcommands = { DelimitedImportSubCommand.class,
		FixedLengthImportSubCommand.class, JsonImportSubCommand.class, DatabaseImportSubCommand.class,
		GeneratorImportSubCommand.class, SimpleGeneratorImportSubCommand.class })
public class ImportCommand extends AbstractCommand<Map<String, Object>, Map<String, Object>> {

}
