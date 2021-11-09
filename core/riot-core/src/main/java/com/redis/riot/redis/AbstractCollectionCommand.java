package com.redis.riot.redis;

import java.util.Map;

import org.springframework.core.convert.converter.Converter;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command
public abstract class AbstractCollectionCommand extends AbstractKeyCommand {

	@Option(names = "--member-space", description = "Keyspace prefix for member IDs", paramLabel = "<str>")
	private String memberSpace;
	@Option(arity = "1..*", names = { "-m",
			"--members" }, description = "Member field names for collections", paramLabel = "<fields>")
	private String[] memberFields;

	protected Converter<Map<String, Object>, String> member() {
		return idMaker(memberSpace, memberFields);
	}

}
