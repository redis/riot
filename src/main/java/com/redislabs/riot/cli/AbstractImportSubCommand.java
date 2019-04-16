package com.redislabs.riot.cli;

import java.io.IOException;
import java.util.Map;

import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;

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
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;

@Command(subcommands = { GeoImportSubSubCommand.class, HashImportSubSubCommand.class, ListImportSubSubCommand.class,
		SearchImportSubSubCommand.class, SetImportSubSubCommand.class, StreamImportSubSubCommand.class,
		StringImportSubSubCommand.class, SuggestImportSubSubCommand.class, ZSetImportSubSubCommand.class })
public abstract class AbstractImportSubCommand extends HelpAwareCommand {

	@ParentCommand
	private ImportCommand parent;
	@Option(names = "--max", description = "Maximum number of items to import.", paramLabel = "<count>", order = 3)
	private Integer maxCount;

	public ItemStreamReader<Map<String, Object>> reader() throws Exception {
		AbstractItemCountingItemStreamItemReader<Map<String, Object>> reader = countingReader();
		if (maxCount != null) {
			reader.setMaxItemCount(maxCount);
		}
		return reader;
	}

	protected abstract AbstractItemCountingItemStreamItemReader<Map<String, Object>> countingReader()
			throws IOException;

	public ImportCommand getParent() {
		return parent;
	}

	public abstract String getSourceDescription();

}
