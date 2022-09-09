package com.redis.riot.redis;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import com.redis.lettucemod.timeseries.DuplicatePolicy;

import picocli.CommandLine.Option;

public class TsAddOptions {

	@Option(names = "--timestamp", description = "Name of the field to use for timestamps. If unset, uses auto-timestamping.", paramLabel = "<field>")
	private Optional<String> timestampField = Optional.empty();
	@Option(names = "--value", required = true, description = "Name of the field to use for values.", paramLabel = "<field>")
	private String valueField;
	@Option(names = "--on-duplicate", description = "Duplicate policy: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<name>")
	private DuplicatePolicy duplicatePolicy = DuplicatePolicy.LAST;
	@Option(arity = "1..*", names = "--labels", description = "Labels in the form label1=field1 label2=field2...", paramLabel = "SPEL")
	private Map<String, String> labels = new LinkedHashMap<>();

	public Optional<String> getTimestampField() {
		return timestampField;
	}

	public void setTimestampField(Optional<String> timestampField) {
		this.timestampField = timestampField;
	}

	public String getValueField() {
		return valueField;
	}

	public void setValueField(String valueField) {
		this.valueField = valueField;
	}

	public DuplicatePolicy getDuplicatePolicy() {
		return duplicatePolicy;
	}

	public void setDuplicatePolicy(DuplicatePolicy duplicatePolicy) {
		this.duplicatePolicy = duplicatePolicy;
	}

	public Map<String, String> getLabels() {
		return labels;
	}

	public void setLabels(Map<String, String> labels) {
		this.labels = labels;
	}

	@Override
	public String toString() {
		return "TsAddOptions [timestampField=" + timestampField + ", valueField=" + valueField + ", duplicatePolicy="
				+ duplicatePolicy + ", labels=" + labels + "]";
	}

}
