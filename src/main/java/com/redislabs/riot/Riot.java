package com.redislabs.riot;

import org.slf4j.impl.SimpleLogger;
import org.springframework.batch.item.file.transform.Range;

import com.redislabs.riot.cli.BaseCommand;
import com.redislabs.riot.cli.ExportParentCommand;
import com.redislabs.riot.cli.ImportParentCommand;
import com.redislabs.riot.cli.ManifestVersionProvider;
import com.redislabs.riot.cli.file.RangeConverter;

import picocli.AutoComplete;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "riot", mixinStandardHelpOptions = true, subcommands = { ImportParentCommand.class,
		ExportParentCommand.class }, versionProvider = ManifestVersionProvider.class)
public class Riot extends BaseCommand {

	@Option(names = "--completion-script", hidden = true)
	private boolean completionScript;

	public static void main(String[] args) {
		System.setProperty(SimpleLogger.LOG_KEY_PREFIX + "io.lettuce.core", "warn");
		int exitCode = new CommandLine(new Riot()).registerConverter(Range.class, new RangeConverter())
				.setCaseInsensitiveEnumValuesAllowed(true).execute(args);
		System.exit(exitCode);
	}

	@Override
	public void run() {
		if (completionScript) {
			System.out.println(AutoComplete.bash("riot", new CommandLine(new Riot())));
		} else {
			super.run();
		}
	}

}
