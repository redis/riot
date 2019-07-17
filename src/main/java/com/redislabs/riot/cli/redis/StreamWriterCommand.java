package com.redislabs.riot.cli.redis;

import com.redislabs.riot.redis.writer.AbstractRedisDataStructureItemWriter;
import com.redislabs.riot.redis.writer.StreamIdMaxlenWriter;
import com.redislabs.riot.redis.writer.StreamIdWriter;
import com.redislabs.riot.redis.writer.StreamMaxlenWriter;
import com.redislabs.riot.redis.writer.StreamWriter;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "stream", description = "Redis stream data structure")
public class StreamWriterCommand extends AbstractDataStructureWriterCommand {

	@Option(names = "--trim", description = "Apply efficient trimming for capped streams using the ~ flag")
	private boolean trim;
	@Option(names = "--max", description = "Limit stream to maxlen entries", paramLabel = "<len>")
	private Long maxlen;
	@Option(names = "--id", description = "Field used for stream entry IDs", paramLabel = "<field>")
	private String idField;

	public AbstractRedisDataStructureItemWriter writer() {
		if (idField == null) {
			if (maxlen == null) {
				return new StreamWriter();
			}
			return new StreamMaxlenWriter(maxlen, trim);
		}
		if (maxlen == null) {
			return new StreamIdWriter(idField);
		}
		return new StreamIdMaxlenWriter(idField, maxlen, trim);
	}
}
