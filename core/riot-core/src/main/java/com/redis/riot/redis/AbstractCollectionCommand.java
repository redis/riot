package com.redis.riot.redis;

import java.util.Map;

import org.springframework.core.convert.converter.Converter;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

@Command
public abstract class AbstractCollectionCommand extends AbstractKeyCommand {

	@Mixin
	private CollectionOptions collectionOptions = new CollectionOptions();

	protected Converter<Map<String, Object>, String> member() {
		return idMaker(collectionOptions.getMemberSpace(), collectionOptions.getMemberFields());
	}

}
