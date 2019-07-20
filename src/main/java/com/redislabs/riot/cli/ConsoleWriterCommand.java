package com.redislabs.riot.cli;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.redislabs.riot.ConsoleWriter;

import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

@Command(name = "console", description = "Console")
public class ConsoleWriterCommand extends AbstractCommand {

	private final static Logger log = LoggerFactory.getLogger(ConsoleWriterCommand.class);

	@ParentCommand
	private AbstractReaderCommand parent;

	@Override
	public void run() {
		try {
			parent.execute(writer(), "console");
		} catch (Exception e) {
			log.debug("Could not create console writer", e);
		}
	}

	private ConsoleWriter writer() {
		return new ConsoleWriter();
	}

}
