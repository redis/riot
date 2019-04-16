package com.redislabs.riot.cli;

import java.io.IOException;
import java.util.Map;

import org.springframework.batch.item.ItemStreamWriter;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;

import com.redislabs.riot.redis.writer.AbstractRedisWriter;

import picocli.CommandLine.ParentCommand;

public abstract class AbstractImportSubSubCommand
		extends AbstractSubSubCommand<Map<String, Object>, Map<String, Object>> {

	@ParentCommand
	private AbstractImportSubCommand parent;

	@Override
	protected AbstractItemCountingItemStreamItemReader<Map<String, Object>> reader() throws IOException {
		return parent.reader();
	}

	@Override
	protected ItemStreamWriter<Map<String, Object>> writer() {
		AbstractRedisWriter writer = redisWriter();
		writer.setPool(parent.getParent().redisConnectionBuilder().buildPool());
		return writer;
	}

	protected abstract AbstractRedisWriter redisWriter();

}
