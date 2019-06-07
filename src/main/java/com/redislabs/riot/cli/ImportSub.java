package com.redislabs.riot.cli;

import java.util.Map;

import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;

import com.redislabs.riot.BaseCommand;
import com.redislabs.riot.cli.redis.GeoImport;
import com.redislabs.riot.cli.redis.HashImport;
import com.redislabs.riot.cli.redis.ListImport;
import com.redislabs.riot.cli.redis.SearchImport;
import com.redislabs.riot.cli.redis.SetImport;
import com.redislabs.riot.cli.redis.StreamImport;
import com.redislabs.riot.cli.redis.StringImport;
import com.redislabs.riot.cli.redis.SuggestImport;
import com.redislabs.riot.cli.redis.ZSetImport;

import lombok.Getter;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;

@Command(subcommands = { GeoImport.class, HashImport.class, ListImport.class, SearchImport.class, SetImport.class,
		StreamImport.class, StringImport.class, SuggestImport.class, ZSetImport.class })
public abstract class ImportSub extends BaseCommand {

	@Option(names = "--max", description = "Maximum number of items to read.", paramLabel = "<count>")
	private Integer count;

	@ParentCommand
	@Getter
	private Import parent;

	public void execute(ItemWriter<Map<String, Object>> writer, String targetDescription) {
		System.out.println("Importing " + getSourceDescription() + " into " + targetDescription);
		try {
			AbstractItemCountingItemStreamItemReader<Map<String, Object>> reader = reader();
			if (count != null) {
				reader.setMaxItemCount(count);
			}
			parent.execute(reader, null, writer);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public abstract AbstractItemCountingItemStreamItemReader<Map<String, Object>> reader() throws Exception;

	public abstract String getSourceDescription();

}
