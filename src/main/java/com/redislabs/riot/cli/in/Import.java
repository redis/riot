package com.redislabs.riot.cli.in;

import java.util.Map;

import com.redislabs.riot.cli.JobCommand;
import com.redislabs.riot.cli.in.file.DelimitedFileImport;
import com.redislabs.riot.cli.in.file.FixedLengthFileImport;
import com.redislabs.riot.cli.in.file.JsonFileImport;

import picocli.CommandLine.Command;

@Command(name = "import", description = "Import into Redis", subcommands = { DelimitedFileImport.class,
		FixedLengthFileImport.class, JsonFileImport.class, DatabaseImport.class, GeneratorImport.class,
		SimpleGeneratorImport.class })
public class Import extends JobCommand<Map<String, Object>, Map<String, Object>> {

}
