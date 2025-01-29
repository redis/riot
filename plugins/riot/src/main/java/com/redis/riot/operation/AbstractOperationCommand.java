package com.redis.riot.operation;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;
import java.util.function.ToLongFunction;

import com.redis.riot.core.BaseCommand;
import com.redis.riot.core.processor.FieldExtractorFactory;
import com.redis.riot.core.processor.IdFunctionBuilder;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command
public abstract class AbstractOperationCommand extends BaseCommand implements OperationCommand {

	public static final String DEFAULT_SEPARATOR = IdFunctionBuilder.DEFAULT_SEPARATOR;

	@Option(names = "--keyspace", description = "Keyspace prefix.", paramLabel = "<str>")
	private String keyspace;

	@Option(names = { "-k", "--key" }, arity = "1..*", description = "Key fields.", paramLabel = "<fields>")
	private List<String> keyFields;

	@Option(names = "--key-separator", description = "Key separator (default: ${DEFAULT-VALUE}).", paramLabel = "<str>")
	private String keySeparator = DEFAULT_SEPARATOR;

	@Option(names = "--remove", description = "Remove key or member fields the first time they are used.")
	private boolean removeFields;

	@Option(names = "--ignore-missing", description = "Ignore missing fields.")
	private boolean ignoreMissingFields;

	public String getKeyspace() {
		return keyspace;
	}

	public void setKeyspace(String keyspace) {
		this.keyspace = keyspace;
	}

	public List<String> getKeyFields() {
		return keyFields;
	}

	public void setKeyFields(String... keys) {
		setKeyFields(Arrays.asList(keys));
	}

	public void setKeyFields(List<String> keyFields) {
		this.keyFields = keyFields;
	}

	public String getKeySeparator() {
		return keySeparator;
	}

	public void setKeySeparator(String keySeparator) {
		this.keySeparator = keySeparator;
	}

	public boolean isRemoveFields() {
		return removeFields;
	}

	public void setRemoveFields(boolean removeFields) {
		this.removeFields = removeFields;
	}

	public boolean isIgnoreMissingFields() {
		return ignoreMissingFields;
	}

	public void setIgnoreMissingFields(boolean ignoreMissingFields) {
		this.ignoreMissingFields = ignoreMissingFields;
	}

	protected Function<Map<String, Object>, String> toString(String field) {
		if (field == null) {
			return s -> null;
		}
		return fieldExtractorFactory().string(field);
	}

	protected FieldExtractorFactory fieldExtractorFactory() {
		return FieldExtractorFactory.builder().remove(removeFields).nullCheck(!ignoreMissingFields).build();
	}

	protected Function<Map<String, Object>, String> idFunction(String prefix, List<String> fields) {
		return new IdFunctionBuilder().separator(keySeparator).remove(removeFields).prefix(prefix).fields(fields)
				.build();
	}

	protected Function<Map<String, Object>, String> keyFunction() {
		return idFunction(keyspace, keyFields);
	}

	protected ToDoubleFunction<Map<String, Object>> score(ScoreArgs args) {
		return toDouble(args.getField(), args.getDefaultValue());
	}

	protected ToDoubleFunction<Map<String, Object>> toDouble(String field, double defaultValue) {
		if (field == null) {
			return m -> defaultValue;
		}
		return fieldExtractorFactory().doubleField(field, defaultValue);
	}

	protected ToLongFunction<Map<String, Object>> toLong(String field) {
		if (field == null) {
			return m -> 0;
		}
		return fieldExtractorFactory().longField(field);
	}

}