package com.redislabs.riot.cli;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemReader;

import com.redislabs.riot.cli.file.FileImportCommand;

import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

@Command
public abstract class ImportCommand extends HelpAwareCommand implements Runnable {

	private final Logger log = LoggerFactory.getLogger(FileImportCommand.class);

	@ParentCommand
	private ImportParentCommand parent;

	@Override
	public void run() {
		ItemReader<Map<String, Object>> reader;
		try {
			reader = reader();
		} catch (Exception e) {
			log.error("Could not initialize reader", e);
			return;
		}
		parent.transfer(reader);
	}

	protected abstract ItemReader<Map<String, Object>> reader() throws Exception;

}
