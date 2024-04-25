package com.redis.riot.cli.redis;

import java.util.LinkedHashMap;
import java.util.Map;

import com.redis.lettucemod.timeseries.DuplicatePolicy;
import com.redis.riot.core.operation.TsAddBuilder;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "ts.add", description = "Add samples to RedisTimeSeries")
public class TsAddCommand extends AbstractRedisOperationCommand {

	@Option(names = "--timestamp", description = "Name of the field to use for timestamps. If unset, uses auto-timestamping.", paramLabel = "<field>")
	private String timestampField;

	@Option(names = "--value", required = true, description = "Name of the field to use for values.", paramLabel = "<field>")
	private String valueField;

	@Option(names = "--on-duplicate", description = "Duplicate policy: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<name>")
	private DuplicatePolicy duplicatePolicy = TsAddBuilder.DEFAULT_DUPLICATE_POLICY;

	@Option(arity = "1..*", names = "--labels", description = "Labels in the form label1=field1 label2=field2...", paramLabel = "SPEL")
	private Map<String, String> labels = new LinkedHashMap<>();

	@Override
	protected TsAddBuilder operationBuilder() {
		TsAddBuilder supplier = new TsAddBuilder();
		supplier.setDuplicatePolicy(duplicatePolicy);
		supplier.setTimestampField(timestampField);
		supplier.setValueField(valueField);
		supplier.setLabels(labels);
		return supplier;
	}

}