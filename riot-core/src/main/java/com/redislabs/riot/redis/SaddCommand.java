package com.redislabs.riot.redis;

import java.util.Map;

import picocli.CommandLine.Command;

@Command(name = "sadd")
public class SaddCommand extends AbstractCollectionCommand {

	@Override
	protected Sadd<String, String, Map<String, Object>> collectionWriter() {
		return new Sadd<>();
	}

}
