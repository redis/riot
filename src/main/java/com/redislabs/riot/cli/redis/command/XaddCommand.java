package com.redislabs.riot.cli.redis.command;

import com.redislabs.riot.redis.writer.map.AbstractKeyMapRedisWriter;
import com.redislabs.riot.redis.writer.map.Xadd;
import com.redislabs.riot.redis.writer.map.XaddId;
import com.redislabs.riot.redis.writer.map.XaddIdMaxlen;
import com.redislabs.riot.redis.writer.map.XaddMaxlen;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "xadd", description = "Append entries to a stream")
public class XaddCommand extends AbstractKeyRedisCommand {

	@Option(names = "--trim", description = "Use efficient trimming (~ flag)")
	private boolean trim;
	@Option(names = "--maxlen", description = "Limit stream to maxlen entries", paramLabel = "<int>")
	private Long maxlen;
	@Option(names = "--id", description = "Field used for stream entry IDs", paramLabel = "<field>")
	private String id;

	@Override
	protected AbstractKeyMapRedisWriter keyWriter() {
		if (id == null) {
			if (maxlen == null) {
				return new Xadd();
			}
			XaddMaxlen xaddMaxlen = new XaddMaxlen();
			xaddMaxlen.approximateTrimming(trim);
			xaddMaxlen.maxlen(maxlen);
			return xaddMaxlen;
		}
		if (maxlen == null) {
			XaddId xaddId = new XaddId();
			xaddId.idField(id);
			return xaddId;
		}
		XaddIdMaxlen xaddIdMaxlen = new XaddIdMaxlen();
		xaddIdMaxlen.approximateTrimming(trim);
		xaddIdMaxlen.maxlen(maxlen);
		xaddIdMaxlen.idField(id);
		return xaddIdMaxlen;
	}

}
