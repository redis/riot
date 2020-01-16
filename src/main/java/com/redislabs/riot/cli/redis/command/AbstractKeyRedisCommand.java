package com.redislabs.riot.cli.redis.command;

import com.redislabs.riot.redis.writer.map.AbstractKeyMapRedisWriter;

import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@SuppressWarnings("rawtypes")
@Command
@Slf4j
public abstract class AbstractKeyRedisCommand extends AbstractRedisCommand {

	@Option(names = "--separator", description = "Key separator (default: ${DEFAULT-VALUE})", paramLabel = "<str>")
	private String separator = ":";
	@Option(names = { "-p", "--keyspace" }, description = "Keyspace prefix", paramLabel = "<str>")
	private String keyspace;
	@Option(names = { "-k", "--keys" }, arity = "1..*", description = "Key fields", paramLabel = "<names>")
	private String[] keys = new String[0];

	@Override
	protected AbstractKeyMapRedisWriter redisWriter() {
		if (keyspace == null && keys.length == 0) {
			log.warn("No keyspace nor key fields specified; using empty key (\"\")");
		}
		AbstractKeyMapRedisWriter writer = keyWriter();
		writer.key(separator, keyspace, keys);
		return writer;
	}

	protected abstract AbstractKeyMapRedisWriter keyWriter();

}
