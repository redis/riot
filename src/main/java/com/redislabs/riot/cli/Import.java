package com.redislabs.riot.cli;

import java.util.Map;

import picocli.CommandLine.Command;

@Command(name = "import", description = "Import into Redis", subcommands = { DelimitedFileImport.class,
		FixedLengthFileImport.class, JsonImport.class, DatabaseImport.class, GeneratorImport.class,
		SimpleGeneratorImport.class })
public class Import extends JobCommand<Map<String, Object>, Map<String, Object>> {

}
