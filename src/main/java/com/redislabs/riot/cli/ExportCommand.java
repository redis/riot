package com.redislabs.riot.cli;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemWriter;

import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

@Command
public abstract class ExportCommand extends HelpAwareCommand implements Runnable {

	private final Logger log = LoggerFactory.getLogger(ExportCommand.class);

	@ParentCommand
	protected ExportParentCommand parent;

	@Override
	public void run() {
		ItemWriter<Map<String, Object>> writer;
		try {
			writer = writer();
		} catch (Exception e) {
			log.error("Could not initialize writer", e);
			return;
		}
		parent.transfer(writer);
	}

	protected abstract ItemWriter<Map<String, Object>> writer() throws Exception;

}
