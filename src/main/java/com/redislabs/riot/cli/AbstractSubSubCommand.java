package com.redislabs.riot.cli;

import java.io.IOException;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStreamWriter;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;

import picocli.CommandLine.ParentCommand;

public abstract class AbstractSubSubCommand<I, O> extends HelpAwareCommand {

	@ParentCommand
	private AbstractSubCommand<I, O> parent;

	@Override
	public Void call() throws Exception {
		parent.getParent().run(parent.getSourceDescription(), reader(), processor(), getTargetDescription(), writer());
		return null;
	}

	protected abstract String getTargetDescription();

	protected ItemProcessor<I, O> processor() {
		return null;
	}

	protected abstract AbstractItemCountingItemStreamItemReader<I> reader() throws IOException;

	protected abstract ItemStreamWriter<O> writer();

}
