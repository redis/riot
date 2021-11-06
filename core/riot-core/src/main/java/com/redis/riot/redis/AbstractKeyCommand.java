package com.redis.riot.redis;

import java.util.Map;

import org.springframework.core.convert.converter.Converter;

import lombok.Data;
import lombok.EqualsAndHashCode;
import picocli.CommandLine;
import picocli.CommandLine.Option;

@Data
@EqualsAndHashCode(callSuper = true)
@CommandLine.Command
public abstract class AbstractKeyCommand extends AbstractRedisCommand<Map<String, Object>> {

	@Option(names = { "-p", "--keyspace" }, description = "Keyspace prefix", paramLabel = "<str>")
	private String keyspace = "";
	@Option(names = { "-k", "--keys" }, arity = "1..*", description = "Key fields", paramLabel = "<fields>")
	private String[] keys;

	protected Converter<Map<String, Object>, String> key() {
		return idMaker(keyspace, keys);
	}

}
