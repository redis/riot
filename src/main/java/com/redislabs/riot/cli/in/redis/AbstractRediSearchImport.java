package com.redislabs.riot.cli.in.redis;

import java.util.Map;

import org.springframework.batch.item.ItemWriter;

import com.redislabs.riot.cli.in.AbstractImportWriterCommand;
import com.redislabs.riot.redis.writer.LettuceAsyncWriter;
import com.redislabs.riot.redis.writer.search.AbstractLettuSearchItemWriter;

import lombok.Getter;
import picocli.CommandLine.Option;

public abstract class AbstractRediSearchImport extends AbstractImportWriterCommand {

	@Getter
	@Option(names = "--keyspace", description = "Document keyspace prefix.")
	private String keyspace;
	@Getter
	@Option(names = "--keys", required = true, arity = "1..*", description = "Document key fields.")
	private String[] keys = new String[0];
	@Getter
	@Option(names = "--index", description = "Name of the RediSearch index", required = true)
	private String index;

	@Override
	protected String getTargetDescription() {
		return "index \"" + index + "\"";
	}

	@Override
	protected ItemWriter<Map<String, Object>> writer() {
		return new LettuceAsyncWriter(getRoot().lettucePool(), lettuceItemWriter());
	}

	protected abstract ItemWriter<Map<String, Object>> jedisSearchWriter();

	private AbstractLettuSearchItemWriter lettuceItemWriter() {
		AbstractLettuSearchItemWriter writer = rediSearchItemWriter();
		writer.setIndex(index);
		writer.setConverter(redisConverter());
		return writer;
	}

	protected abstract AbstractLettuSearchItemWriter rediSearchItemWriter();

}
