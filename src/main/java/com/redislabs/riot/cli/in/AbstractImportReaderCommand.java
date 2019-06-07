package com.redislabs.riot.cli.in;

import java.util.Map;

import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;

import com.redislabs.riot.cli.AbstractCommand;
import com.redislabs.riot.cli.in.redis.GeoImport;
import com.redislabs.riot.cli.in.redis.HashImport;
import com.redislabs.riot.cli.in.redis.ListImport;
import com.redislabs.riot.cli.in.redis.SearchImport;
import com.redislabs.riot.cli.in.redis.SetImport;
import com.redislabs.riot.cli.in.redis.StreamImport;
import com.redislabs.riot.cli.in.redis.StringImport;
import com.redislabs.riot.cli.in.redis.SuggestImport;
import com.redislabs.riot.cli.in.redis.ZSetImport;

import lombok.Getter;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;

@Command(subcommands = { GeoImport.class, HashImport.class, ListImport.class, SearchImport.class, SetImport.class,
		StreamImport.class, StringImport.class, SuggestImport.class, ZSetImport.class })
public abstract class AbstractImportReaderCommand extends AbstractCommand {

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
