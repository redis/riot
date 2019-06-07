package com.redislabs.riot.cli.redis;

import com.redislabs.riot.redis.writer.RedisItemWriter;
import com.redislabs.riot.redis.writer.StringWriter;
import com.redislabs.riot.redis.writer.StringWriter.StringFormat;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "string", description = "String data structure")
public class StringImport extends AbstractSingleImport {

	@Option(names = "--format", description = "Serialization format: ${COMPLETION-CANDIDATES}")
	private StringFormat format = StringFormat.Json;
	@Option(names = "--root", description = "XML root element name.")
	private String root;

	@Override
	protected RedisItemWriter redisItemWriter() {
		StringWriter writer = new StringWriter();
		writer.setFormat(format).withRootName(root);
		return writer;
	}

	@Override
	protected String getDataStructure() {
		return "strings";
	}

}
