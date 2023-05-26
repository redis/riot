package com.redis.riot.cli.operation;

import java.util.Map;
import java.util.function.Function;

import picocli.CommandLine.Mixin;

public abstract class AbstractKeyCommand extends AbstractOperationCommand<Map<String, Object>> {

	@Mixin
	private KeyOptions keyOptions = new KeyOptions();

	protected Function<Map<String, Object>, String> key() {
		return idMaker(keyOptions.getKeyspace(), keyOptions.getKeys());
	}

	public KeyOptions getKeyOptions() {
		return keyOptions;
	}

	public void setKeyOptions(KeyOptions keyOptions) {
		this.keyOptions = keyOptions;
	}

}
