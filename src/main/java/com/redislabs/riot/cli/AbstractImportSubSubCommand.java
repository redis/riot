package com.redislabs.riot.cli;

import java.io.IOException;
import java.util.Map;

import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.beans.factory.InitializingBean;

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
	protected AbstractRedisWriter writer() throws Exception {
		AbstractRedisWriter writer = createWriter();
		writer.setPool(parent.getParent().redisConnectionBuilder().buildPool());
		if (writer instanceof InitializingBean) {
			((InitializingBean) writer).afterPropertiesSet();
		}
		return writer;
	}

	protected abstract AbstractRedisWriter createWriter();

}
