package com.redis.riot.command;

import java.util.Map;

import org.springframework.core.convert.converter.Converter;

import picocli.CommandLine.Mixin;

public abstract class AbstractKeyCommand extends AbstractOperationCommand<Map<String, Object>> {

	@Mixin
	private KeyOptions keyOptions = new KeyOptions();

	protected Converter<Map<String, Object>, String> key() {
		return idMaker(keyOptions.getKeyspace(), keyOptions.getKeys());
	}

	public KeyOptions getKeyOptions() {
		return keyOptions;
	}

	public void setKeyOptions(KeyOptions keyOptions) {
		this.keyOptions = keyOptions;
	}

}
