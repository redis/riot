package com.redislabs.riot.cli;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;

import com.redislabs.riot.Processor;
import com.redislabs.riot.RiotApplication;
import com.redislabs.riot.ThrottledItemStreamReader;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;

@Command(subcommands = { RedisDataStructureWriterCommand.class, RediSearchWriterCommand.class,  FileWriterCommand.class,
		DatabaseWriterCommand.class }, synopsisSubcommandLabel = "[TARGET]", commandListHeading = "Targets:%n")
public abstract class AbstractReaderCommand extends AbstractCommand {

	private final static Logger log = LoggerFactory.getLogger(AbstractReaderCommand.class);

	@Option(names = "--max", description = "Max number of items to read", paramLabel = "<count>")
	private Integer count;
	@Option(names = "--sleep", description = "Sleep duration in millis between reads", paramLabel = "<millis>")
	private Long sleep;
	@Option(names = "--processor", description = "SpEL expression to process a field", paramLabel = "<name=SpEL>")
	private Map<String, String> processorFields;
	@ParentCommand
	private RiotApplication root;

	public void execute(ItemWriter<Map<String, Object>> writer, String targetDescription) {
		try {
			AbstractItemCountingItemStreamItemReader<Map<String, Object>> reader = reader();
			if (count != null) {
				reader.setMaxItemCount(count);
			}
			System.out.println("Transferring from " + description() + " to " + targetDescription);
			root.execute(throttle(reader), processor(), writer);
		} catch (Exception e) {
			System.err.println(e.getMessage());
			log.debug("Could not execute transfer", e);
		}
	}

	private ItemStreamReader<Map<String, Object>> throttle(ItemStreamReader<Map<String, Object>> reader) {
		if (sleep == null) {
			return reader;
		}
		return new ThrottledItemStreamReader<>(reader, sleep);
	}

	private ItemProcessor<Map<String, Object>, Map<String, Object>> processor() throws Exception {
		if (processorFields == null) {
			return null;
		}
		return new Processor(processorFields);
	}

	protected abstract AbstractItemCountingItemStreamItemReader<Map<String, Object>> reader() throws Exception;

	protected abstract String description();

}
