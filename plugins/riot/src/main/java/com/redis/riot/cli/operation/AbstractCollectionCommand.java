package com.redis.riot.cli.operation;

import java.util.Map;
import java.util.function.Function;

import picocli.CommandLine.Mixin;

public abstract class AbstractCollectionCommand extends AbstractKeyCommand {

	@Mixin
	private CollectionOptions collectionOptions = new CollectionOptions();

	protected Function<Map<String, Object>, String> member() {
		return idMaker(collectionOptions.getMemberSpace(), collectionOptions.getMemberFields());
	}

}
