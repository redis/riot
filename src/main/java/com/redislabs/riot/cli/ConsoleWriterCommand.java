package com.redislabs.riot.cli;

import com.redislabs.riot.batch.LoggingWriter;

public class ConsoleWriterCommand {

	public LoggingWriter writer() {
		return new LoggingWriter();
	}

	protected String targetDescription() {
		return "console";
	}

}
