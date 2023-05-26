package com.redis.riot.cli.operation;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import com.redis.riot.cli.common.HelpOptions;
import com.redis.riot.core.convert.FieldExtractorFactory;
import com.redis.riot.core.convert.IdConverterBuilder;
import com.redis.riot.core.convert.ObjectToNumberConverter;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

@Command
public abstract class AbstractOperationCommand<O> implements OperationCommand<O> {

	@Mixin
	private HelpOptions helpOptions = new HelpOptions();
	@Mixin
	private RedisCommandOptions commandOptions = new RedisCommandOptions();

	public RedisCommandOptions getCommandOptions() {
		return commandOptions;
	}

	public void setCommandOptions(RedisCommandOptions commandOptions) {
		this.commandOptions = commandOptions;
	}

	protected Function<Map<String, Object>, Double> doubleFieldExtractor(String field) {
		Function<Map<String, Object>, Object> extractor = fieldExtractorFactory().field(field);
		return extractor.andThen(new ObjectToNumberConverter<>(Double.class));
	}

	protected Function<Map<String, Object>, String> stringFieldExtractor(Optional<String> field) {
		if (field.isPresent()) {
			return stringFieldExtractor(field.get());
		}
		return s -> null;
	}

	protected Function<Map<String, Object>, String> stringFieldExtractor(String field) {
		return fieldExtractorFactory().string(field);
	}

	private FieldExtractorFactory fieldExtractorFactory() {
		return FieldExtractorFactory.builder().remove(commandOptions.isRemoveFields())
				.nullCheck(!commandOptions.isIgnoreMissingFields()).build();
	}

	protected <T extends Number> Function<Map<String, Object>, T> numberExtractor(String field, Class<T> targetType) {
		return fieldExtractorFactory().field(field).andThen(new ObjectToNumberConverter<>(targetType));
	}

	protected <T extends Number> Function<Map<String, Object>, T> numberExtractor(Optional<String> field,
			Class<T> targetType, T defaultValue) {
		if (field.isPresent()) {
			return numberExtractor(field.get(), targetType, defaultValue);
		}
		return s -> defaultValue;

	}

	protected <T extends Number> Function<Map<String, Object>, T> numberExtractor(String field, Class<T> targetType,
			Object defaultValue) {
		return fieldExtractorFactory().field(field, defaultValue).andThen(new ObjectToNumberConverter<>(targetType));
	}

	protected Function<Map<String, Object>, String> idMaker(Optional<String> prefix, String... fields) {
		return new IdConverterBuilder().separator(commandOptions.getKeySeparator())
				.remove(commandOptions.isRemoveFields()).prefix(prefix).fields(fields).build();
	}

}
