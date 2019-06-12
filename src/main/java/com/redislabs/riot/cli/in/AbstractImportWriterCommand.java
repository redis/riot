package com.redislabs.riot.cli.in;

import java.util.Map;

import org.springframework.batch.item.ItemWriter;

import com.redislabs.riot.cli.AbstractCommand;
import com.redislabs.riot.cli.RootCommand;
import com.redislabs.riot.redis.RedisConverter;

import lombok.Getter;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;

public abstract class AbstractImportWriterCommand extends AbstractCommand {

	@ParentCommand
	@Getter
	private AbstractImportReaderCommand parent;
	@Getter
	@Option(names = "--separator", description = "Redis key separator. (default: ${DEFAULT-VALUE}).")
	private String separator = ":";

	protected RedisConverter redisConverter() {
		return new RedisConverter(getSeparator(), getKeyspace(), getKeys());
	}

	protected abstract String getKeyspace();

	protected abstract String[] getKeys();

	@Override
	public void run() {
		parent.execute(writer(), getTargetDescription());
	}

	protected abstract ItemWriter<Map<String, Object>> writer();

	protected abstract String getTargetDescription();

	protected RootCommand getRoot() {
		return parent.getParent().getParent();
	}

}
