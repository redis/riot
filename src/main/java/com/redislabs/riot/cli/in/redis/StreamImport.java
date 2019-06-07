package com.redislabs.riot.cli.in.redis;

import com.redislabs.riot.cli.in.AbstractImportWriterCommand;
import com.redislabs.riot.redis.writer.RedisItemWriter;
import com.redislabs.riot.redis.writer.StreamWriter;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "stream", description = "Stream data structure")
public class StreamImport extends AbstractImportWriterCommand {

	@Option(names = "--keyspace", required = true, description = "Redis keyspace prefix.")
	private String keyspace;
	@Option(names = "--keys", arity = "1..*", description = "Key fields.")
	private String[] keys = new String[0];
	@Option(names = "--approximate-trimming", description = "Apply efficient trimming for capped streams using the ~ flag.")
	private boolean approximateTrimming;
	@Option(names = "--maxlen", description = "Limit stream to maxlen entries.")
	private Long maxlen;
	@Option(names = "--id", description = "Field used for stream entry IDs.")
	private String id;

	@Override
	protected String getKeyspace() {
		return keyspace;
	}

	@Override
	protected String[] getKeys() {
		return keys;
	}

	@Override
	protected RedisItemWriter redisItemWriter() {
		StreamWriter writer = new StreamWriter();
		writer.setApproximateTrimming(approximateTrimming);
		writer.setIdField(id);
		writer.setMaxlen(maxlen);
		return writer;
	}

	@Override
	protected String getDataStructure() {
		return "stream";
	}

}
