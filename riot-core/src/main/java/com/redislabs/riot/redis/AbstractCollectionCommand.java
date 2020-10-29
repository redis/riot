package com.redislabs.riot.redis;

import java.util.Map;

import org.springframework.batch.item.redis.support.AbstractCollectionCommandItemWriterBuilder;

import picocli.CommandLine.Option;

public abstract class AbstractCollectionCommand extends AbstractKeyCommand {

	@Option(names = "--member-space", description = "Keyspace prefix for member IDs", paramLabel = "<str>")
	private String memberSpace;
	@Option(names = { "-m",
			"--members" }, arity = "1..*", description = "Member field names for collections", paramLabel = "<fields>")
	private String[] memberFields = new String[0];

	protected <B extends AbstractCollectionCommandItemWriterBuilder<String, String, Map<String, Object>, B>> B configure(
			B builder) throws Exception {
		return super.configure(builder).memberIdConverter(idMaker(memberSpace, memberFields));
	}

}
