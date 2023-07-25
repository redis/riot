package com.redis.riot.cli.operation;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;
import java.util.function.ToLongFunction;

import com.redis.riot.cli.common.HelpOptions;
import com.redis.riot.core.convert.FieldExtractorFactory;
import com.redis.riot.core.convert.IdConverterBuilder;

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

	protected ToLongFunction<Map<String, Object>> longExtractor(Optional<String> field, long defaultValue) {
		if (field.isPresent()) {
			return fieldExtractorFactory().longField(field.get(), defaultValue);
		}
		return m -> defaultValue;
	}

	protected ToDoubleFunction<Map<String, Object>> doubleExtractor(Optional<String> field, double defaultValue) {
		if (field.isPresent()) {
			return fieldExtractorFactory().doubleField(field.get(), defaultValue);
		}
		return m -> defaultValue;
	}

	protected Function<Map<String, Object>, String> idMaker(Optional<String> prefix, String... fields) {
		return new IdConverterBuilder().separator(commandOptions.getKeySeparator())
				.remove(commandOptions.isRemoveFields()).prefix(prefix).fields(fields).build();
	}

}
