package com.redislabs.riot;

import org.slf4j.impl.SimpleLogger;
import org.springframework.batch.item.file.transform.Range;

import com.redislabs.riot.cli.ManifestVersionProvider;
import com.redislabs.riot.cli.db.DatabaseExportCommand;
import com.redislabs.riot.cli.db.DatabaseImportCommand;
import com.redislabs.riot.cli.file.FileExportCommand;
import com.redislabs.riot.cli.file.FileImportCommand;
import com.redislabs.riot.cli.file.RangeConverter;
import com.redislabs.riot.cli.generator.GeneratorImportCommand;

import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "riot", mixinStandardHelpOptions = true, subcommands = { FileImportCommand.class,
		FileExportCommand.class, DatabaseExportCommand.class, DatabaseImportCommand.class,
		GeneratorImportCommand.class }, versionProvider = ManifestVersionProvider.class)
public class Riot implements Runnable {

	public static void main(String[] args) {
		System.setProperty(SimpleLogger.LOG_KEY_PREFIX + "io.lettuce.core", "warn");
		int exitCode = new CommandLine(new Riot()).registerConverter(Range.class, new RangeConverter())
				.setCaseInsensitiveEnumValuesAllowed(true).execute(args);
		System.exit(exitCode);
	}

	@Override
	public void run() {
		CommandLine.usage(this, System.out);
	}

}
