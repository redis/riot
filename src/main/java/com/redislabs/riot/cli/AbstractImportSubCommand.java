package com.redislabs.riot.cli;

import java.io.IOException;
import java.util.Map;

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

@Command(subcommands = { GeoImportSubSubCommand.class, HashImportSubSubCommand.class, ListImportSubSubCommand.class,
		SearchImportSubSubCommand.class, SetImportSubSubCommand.class, StreamImportSubSubCommand.class,
		StringImportSubSubCommand.class, SuggestImportSubSubCommand.class, ZSetImportSubSubCommand.class })
public abstract class AbstractImportSubCommand extends AbstractSubCommand<Map<String, Object>, Map<String, Object>> {

	public abstract AbstractItemCountingItemStreamItemReader<Map<String, Object>> reader() throws IOException;

}
