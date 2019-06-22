package com.redislabs.riot.cli;

import java.util.Map;

import org.springframework.batch.item.ItemWriter;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine.ParentCommand;

@Slf4j
public abstract class AbstractWriterCommand extends AbstractCommand {

	@ParentCommand
	@Getter
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
