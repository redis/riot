package com.redislabs.riot.redis;

import java.util.Map;

import picocli.CommandLine.Option;

public abstract class AbstractCollectionCommand extends AbstractKeyCommand {

	@Option(names = "--member-space", description = "Keyspace prefix for member IDs", paramLabel = "<str>")
	private String memberSpace;
	@Option(names = { "-m",
			"--members" }, arity = "1..*", description = "Member field names for collections", paramLabel = "<fields>")
	private String[] memberFields = new String[0];

	@Override
	protected AbstractCollectionWriter<String, String, Map<String, Object>> keyWriter() {
		AbstractCollectionWriter<String, String, Map<String, Object>> writer = collectionWriter();
		writer.setMemberIdConverter(idMaker(memberSpace, memberFields));
		return writer;
	}

	protected abstract AbstractCollectionWriter<String, String, Map<String, Object>> collectionWriter();

}
