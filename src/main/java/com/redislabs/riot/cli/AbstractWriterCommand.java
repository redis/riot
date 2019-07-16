package com.redislabs.riot.cli;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemWriter;

import picocli.CommandLine.ParentCommand;

public abstract class AbstractWriterCommand extends AbstractCommand {

	private final static Logger log = LoggerFactory.getLogger(AbstractWriterCommand.class);

	@ParentCommand
	private AbstractReaderCommand parent;

	@Override
	public void run() {
		System.out.println("Reading from " + parent.getSourceDescription() + " into " + getTargetDescription());
		ItemWriter<Map<String, Object>> writer = writer();
		try {
			parent.execute(writer);
		} catch (Exception e) {
			System.err.println(e.getMessage());
			log.debug("Could not execute transfer", e);
		}
	}

	protected abstract ItemWriter<Map<String, Object>> writer();

	protected abstract String getTargetDescription();

}
