package com.redislabs.riot.cli.redis;

import com.redislabs.riot.cli.AbstractImportSubSubCommand;
import com.redislabs.riot.redis.writer.AbstractRedisItemWriter;
import com.redislabs.riot.redis.writer.StreamWriter;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "stream", description = "Stream data structure")
public class StreamImportSubSubCommand extends AbstractImportSubSubCommand {

	@Option(names = "--keyspace", required = true, description = "Redis keyspace prefix.")
	private String keyspace;
	@Option(names = "--keys", arity = "1..*", description = "Key fields.")
	private String[] keys = new String[0];
	@Option(names = "--approximate-trimming", description = "Apply efficient trimming for capped streams using the ~ flag.")
	private boolean approximateTrimming;
	@Option(names = "--maxlen", description = "Limit stream to maxlen entries.")
	private Long maxlen;
	@Option(names = "--id", description = "Field used for stream entry IDs.")
	private String idField;

	@Override
	protected String getKeyspace() {
		return keyspace;
	}

	@Override
	protected String[] getKeys() {
		return keys;
	}

	@Override
	protected AbstractRedisItemWriter redisItemWriter() {
		StreamWriter writer = new StreamWriter();
		writer.setApproximateTrimming(approximateTrimming);
		writer.setIdField(idField);
		writer.setMaxlen(maxlen);
		return writer;
	}

	@Override
	protected String getDataStructure() {
		return "stream";
	}

}
