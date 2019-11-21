package com.redislabs.riot.cli.redis;

import com.redislabs.riot.batch.redis.map.AbstractMapWriter;
import com.redislabs.riot.batch.redis.map.XaddIdMapWriter;
import com.redislabs.riot.batch.redis.map.XaddIdMaxlenMapWriter;
import com.redislabs.riot.batch.redis.map.XaddMapWriter;
import com.redislabs.riot.batch.redis.map.XaddMaxlenMapWriter;

import lombok.Data;
import picocli.CommandLine.Option;

public @Data class StreamCommandOptions {

	@Option(names = "--xadd-trim", description = "Use efficient trimming (~ flag)")
	private boolean xaddTrim;
	@Option(names = "--xadd-maxlen", description = "Limit stream to maxlen entries", paramLabel = "<int>")
	private Long xaddMaxlen;
	@Option(names = "--xadd-id", description = "Field used for stream entry IDs", paramLabel = "<field>")
	private String xaddId;

	public <R> AbstractMapWriter<R> writer() {
		if (xaddId == null) {
			if (xaddMaxlen == null) {
				return new XaddMapWriter<R>();
			}
			return new XaddMaxlenMapWriter<R>().approximateTrimming(xaddTrim).maxlen(xaddMaxlen);
		}
		if (xaddMaxlen == null) {
			return new XaddIdMapWriter<R>().idField(xaddId);
		}
		return new XaddIdMaxlenMapWriter<R>().approximateTrimming(xaddTrim).idField(xaddId).maxlen(xaddMaxlen);
	}

}
