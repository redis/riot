package com.redislabs.riot.cli;

import java.util.Map;

import org.springframework.batch.item.ItemWriter;

import com.redislabs.riot.BaseCommand;
import com.redislabs.riot.redis.RedisReader;

import picocli.CommandLine.ParentCommand;

public abstract class ExportSub extends BaseCommand {

	@ParentCommand
	private Export parent;

	private RedisReader reader() {
		RedisReader reader = new RedisReader();
		reader.setJedisPool(parent.getParent().redisConnectionBuilder().buildJedisPool());
		reader.setCount(parent.getScanCount());
		reader.setMatch(getScanPattern());
		reader.setKeys(parent.getKeys());
		reader.setKeyspace(parent.getKeyspace());
		reader.setSeparator(parent.getSeparator());
		return reader;
	}

	private String getScanPattern() {
		if (parent.getKeyspace() == null) {
			return null;
		}
		return parent.getKeyspace() + parent.getSeparator() + "*";
	}

	@Override
	public void run() {
		parent.execute(reader(), null, writer());
	}

	protected abstract ItemWriter<Map<String, Object>> writer();
}
