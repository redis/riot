package com.redis.riot.redis;

import java.util.Map;

import org.springframework.core.convert.converter.Converter;

import com.redis.spring.batch.support.convert.KeyMaker;

import picocli.CommandLine;
import picocli.CommandLine.Option;

@CommandLine.Command
public abstract class AbstractCollectionCommand extends AbstractKeyCommand {

	@Option(names = "--member-space", description = "Keyspace prefix for member IDs", paramLabel = "<str>")
	private String memberSpace = "";
	@Option(arity = "1..*", names = { "-m",
			"--members" }, description = "Member field names for collections", paramLabel = "<fields>")
	private String[] memberFields;

	protected KeyMaker<Map<String, Object>> member() {
		return idMaker(memberSpace, memberFields);
	}

	protected Converter<Map<String, Object>, String[]> members() {
		KeyMaker<Map<String, Object>> member = member();
		return m -> new String[] { member.convert(m) };
	}

}
