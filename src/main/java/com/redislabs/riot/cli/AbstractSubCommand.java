package com.redislabs.riot.cli;

import picocli.CommandLine.ParentCommand;

public abstract class AbstractSubCommand<I, O> extends HelpAwareCommand {

	@ParentCommand
	private AbstractCommand parent;

	public AbstractCommand getParent() {
		return parent;
	}

	public abstract String getSourceDescription();

}
