package com.redislabs.riot.cli;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStreamWriter;

import picocli.CommandLine.ParentCommand;

public abstract class AbstractSubSubCommand<I, O> extends BaseCommand {

	@ParentCommand
	private AbstractSubCommand<I, O> parent;

	@Override
	public void run() {
		if (isHelpRequested()) {
			super.run();
		}
		try {
			parent.getParent().run(parent.getSourceDescription(), parent.reader(), processor(), getTargetDescription(),
					writer());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	protected abstract String getTargetDescription();

	protected ItemProcessor<I, O> processor() {
		return null;
	}

	protected abstract ItemStreamWriter<O> writer();

}