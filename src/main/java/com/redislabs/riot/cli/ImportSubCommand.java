package com.redislabs.riot.cli;

import java.util.Map;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.item.ItemStreamWriter;
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

import lombok.Getter;
import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

@Command(subcommands = { GeoImportSubSubCommand.class, HashImportSubSubCommand.class, ListImportSubSubCommand.class,
		SearchImportSubSubCommand.class, SetImportSubSubCommand.class, StreamImportSubSubCommand.class,
		StringImportSubSubCommand.class, SuggestImportSubSubCommand.class, ZSetImportSubSubCommand.class })
public abstract class ImportSubCommand extends BaseCommand {

	@ParentCommand
	@Getter
	private ImportCommand parent;

	public ExitStatus call(ItemStreamWriter<Map<String, Object>> writer) throws Exception {
		return parent.call(reader(), null, writer);
	}

	public abstract AbstractItemCountingItemStreamItemReader<Map<String, Object>> reader() throws Exception;

}
