package com.redislabs.riot.cli;

import java.util.Map;

import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;

import com.redislabs.riot.Riot;

import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.ParentCommand;
import picocli.CommandLine.Spec;

@Slf4j
@Command
public abstract class ExportCommand extends TransferCommand implements Runnable {

	@ParentCommand
	private Riot parent;
	@Spec
	private CommandSpec spec;
	@ArgGroup(exclusive = false, heading = "Redis reader options%n")
	private RedisReaderOptions options = new RedisReaderOptions();

	@Override
	public void run() {
		ItemReader<Map<String, Object>> reader = options.reader(parent.redisOptions());
		ItemWriter<Map<String, Object>> writer;
		try {
			writer = writer();
		} catch (Exception e) {
			log.error("Could not initialize writer", e);
			return;
		}
		execute(reader, writer);
	}

	protected abstract ItemWriter<Map<String, Object>> writer() throws Exception;

}
