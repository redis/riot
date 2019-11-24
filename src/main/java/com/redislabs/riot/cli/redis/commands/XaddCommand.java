package com.redislabs.riot.cli.redis.commands;

import com.redislabs.riot.batch.redis.writer.AbstractKeyRedisWriter;
import com.redislabs.riot.batch.redis.writer.Xadd;
import com.redislabs.riot.batch.redis.writer.XaddId;
import com.redislabs.riot.batch.redis.writer.XaddIdMaxlen;
import com.redislabs.riot.batch.redis.writer.XaddMaxlen;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "xadd", description="Append entries to a stream")
public class XaddCommand extends AbstractKeyRedisCommand {

	@Option(names = "--trim", description = "Use efficient trimming (~ flag)")
	private boolean trim;
	@Option(names = "--maxlen", description = "Limit stream to maxlen entries", paramLabel = "<int>")
	private Long maxlen;
	@Option(names = "--id", description = "Field used for stream entry IDs", paramLabel = "<field>")
	private String id;
	
	@SuppressWarnings("rawtypes")
	@Override
	protected AbstractKeyRedisWriter keyWriter() {
		if (id == null) {
			if (maxlen == null) {
				return new Xadd();
			}
			return new XaddMaxlen().approximateTrimming(trim).maxlen(maxlen);
		}
		if (maxlen == null) {
			return new XaddId().idField(id);
		}
		return new XaddIdMaxlen().approximateTrimming(trim).maxlen(maxlen).idField(id);
	}

}
