package com.redislabs.riot.cli;

import java.io.IOException;

import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;

import picocli.CommandLine.ParentCommand;

public abstract class AbstractSubCommand<I, O> extends HelpAwareCommand {

	@ParentCommand
	private AbstractCommand<I, O> parent;

	public AbstractCommand<I, O> getParent() {
		return parent;
	}

	public abstract String getSourceDescription();

	public abstract AbstractItemCountingItemStreamItemReader<I> reader() throws IOException;

}
