package com.redislabs.riot.cli.in;

import java.util.Map;

import com.redislabs.riot.cli.AbstractSubCommand;
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

}
