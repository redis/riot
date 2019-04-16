package com.redislabs.riot.cli;

import java.util.Map;

import org.springframework.batch.item.ItemStreamReader;

import com.redislabs.riot.cli.redis.GeoImportSubSubCommand;
import com.redislabs.riot.cli.redis.HashImportSubSubCommand;
import com.redislabs.riot.cli.redis.ListImportSubSubCommand;
import com.redislabs.riot.cli.redis.SearchImportSubSubCommand;
import com.redislabs.riot.cli.redis.SetImportSubSubCommand;
import com.redislabs.riot.cli.redis.StreamImportSubSubCommand;
import com.redislabs.riot.cli.redis.StringImportSubSubCommand;
import com.redislabs.riot.cli.redis.SuggestImportSubSubCommand;
import com.redislabs.riot.cli.redis.ZSetImportSubSubCommand;

import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

@Command(subcommands = { GeoImportSubSubCommand.class, HashImportSubSubCommand.class, ListImportSubSubCommand.class,
		SearchImportSubSubCommand.class, SetImportSubSubCommand.class, StreamImportSubSubCommand.class,
		StringImportSubSubCommand.class, SuggestImportSubSubCommand.class, ZSetImportSubSubCommand.class })
public abstract class AbstractImportSubCommand extends HelpAwareCommand {

	@ParentCommand
	private ImportCommand parent;

	public abstract ItemStreamReader<Map<String, Object>> reader() throws Exception;

	public ImportCommand getParent() {
		return parent;
	}

	public abstract String getSourceDescription();

}
