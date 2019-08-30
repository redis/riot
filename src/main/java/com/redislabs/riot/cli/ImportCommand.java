package com.redislabs.riot.cli;

import java.util.Map;

import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemReader;

import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

@Command
public abstract class ImportCommand extends HelpAwareCommand implements Runnable {

	@ParentCommand
	private ImportParentCommand parent;

	@Override
	public void run() {
		ItemReader<Map<String, Object>> reader;
		try {
			reader = reader();
		} catch (Exception e) {
			LoggerFactory.getLogger(ImportCommand.class).error("Could not initialize reader", e);
			return;
		}
		parent.transfer(reader);
	}

	protected abstract ItemReader<Map<String, Object>> reader() throws Exception;

}
