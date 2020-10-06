package com.redislabs.riot.redis;

import java.util.Map;

import picocli.CommandLine.Command;

@Command(name = "rpush")
public class RpushCommand extends AbstractCollectionCommand {

	@Override
	protected Rpush<String, String, Map<String, Object>> collectionWriter() {
		return new Rpush<>();
	}

}
