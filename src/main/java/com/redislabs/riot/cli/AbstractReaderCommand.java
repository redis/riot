package com.redislabs.riot.cli;

import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;

import com.redislabs.riot.Processor;
import com.redislabs.riot.RiotApplication;
import com.redislabs.riot.ThrottledItemStreamReader;
import com.redislabs.riot.cli.file.DelimitedFileWriterCommand;
import com.redislabs.riot.cli.file.FormattedFileWriterCommand;
import com.redislabs.riot.cli.file.JsonFileWriterCommand;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;

@Command(subcommands = { RedisDataStructureWriterCommand.class, RediSearchWriterCommand.class,
		DelimitedFileWriterCommand.class, FormattedFileWriterCommand.class, JsonFileWriterCommand.class,
		DatabaseWriterCommand.class }, synopsisSubcommandLabel = "[TARGET]", commandListHeading = "Targets:%n")
public abstract class AbstractReaderCommand extends AbstractCommand {

	@Option(names = "--max", description = "Max number of items to read.", paramLabel = "<count>")
	private Integer count;
	@Option(names = "--sleep", description = "Sleep duration in millis between reads.", paramLabel = "<millis>")
	private Long sleep;
	@Option(names = "--processor", description = "SpEL expression to process a field.", paramLabel = "<name=SpEL>")
	private Map<String, String> processorFields = new LinkedHashMap<>();
	@ParentCommand
	private RiotApplication root;

	protected void execute(ItemWriter<Map<String, Object>> writer) throws Exception {
		AbstractItemCountingItemStreamItemReader<Map<String, Object>> reader = reader();
		if (count != null) {
			reader.setMaxItemCount(count);
		}
		root.execute(throttle(reader), processor(), writer);
	}

	private ItemStreamReader<Map<String, Object>> throttle(ItemStreamReader<Map<String, Object>> reader) {
		if (sleep == null) {
			return reader;
		}
		return new ThrottledItemStreamReader<>(reader, sleep);
	}

	private ItemProcessor<Map<String, Object>, Map<String, Object>> processor() throws Exception {
		if (processorFields.isEmpty()) {
			return null;
		}
		return new Processor(processorFields);
	}

	public abstract AbstractItemCountingItemStreamItemReader<Map<String, Object>> reader() throws Exception;

	public abstract String getSourceDescription();

}
