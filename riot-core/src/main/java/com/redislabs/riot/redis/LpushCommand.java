package com.redislabs.riot.redis;

import java.util.Map;

import picocli.CommandLine.Command;

@Command(name = "lpush")
public class LpushCommand extends AbstractCollectionCommand {

	@Override
	protected Lpush<String, String, Map<String, Object>> collectionWriter() {
		return new Lpush<>();
	}

}
