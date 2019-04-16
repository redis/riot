package com.redislabs.riot.cli.redis;

import com.redislabs.riot.redis.writer.StreamWriter;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "stream", description = "Stream data structure")
public class StreamImportSubSubCommand extends AbstractRedisDataStructureImportSubSubCommand {

	@Option(names = "--approximate-trimming", description = "Apply efficient trimming for capped streams using the ~ flag.", order = 5)
	private boolean approximateTrimming;
	@Option(names = "--maxlen", description = "Limit stream to maxlen entries.", order = 5)
	private Long maxlen;
	@Option(names = "--id", description = "Field used for stream entry IDs.", order = 5)
	private String idField;

	@Override
	protected StreamWriter doCreateWriter() {
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
