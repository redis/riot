package com.redislabs.riot.redis;

import java.util.Map;

import picocli.CommandLine;

public abstract class AbstractCollectionCommand extends AbstractKeyCommand {

	@CommandLine.Option(names = "--member-space", description = "Keyspace prefix for member IDs", paramLabel = "<str>")
	String memberSpace;
	@CommandLine.Option(names = { "-m",
			"--members" }, arity = "1..*", description = "Member field names for collections", paramLabel = "<fields>")
	String[] memberFields = new String[0];

	@Override
	protected AbstractCollectionWriter<String, String, Map<String, Object>> keyWriter() {
		AbstractCollectionWriter<String, String, Map<String, Object>> writer = collectionWriter();
		writer.setMemberIdConverter(idMaker(memberSpace, memberFields));
		return writer;
	}

	protected abstract AbstractCollectionWriter<String, String, Map<String, Object>> collectionWriter();

}
