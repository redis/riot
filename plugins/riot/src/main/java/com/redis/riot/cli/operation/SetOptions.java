package com.redis.riot.cli.operation;

import java.util.Optional;

import picocli.CommandLine.Option;

public class SetOptions {

	public enum StringFormat {
		RAW, XML, JSON
	}

	@Option(names = "--format", description = "Serialization: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<fmt>")
	private StringFormat format = StringFormat.JSON;
	@Option(names = "--field", description = "Raw value field.", paramLabel = "<field>")
	private Optional<String> field = Optional.empty();
	@Option(names = "--root", description = "XML root element name.", paramLabel = "<name>")
	private String root;

	public StringFormat getFormat() {
		return format;
	}

	public void setFormat(StringFormat format) {
		this.format = format;
	}

	public Optional<String> getField() {
		return field;
	}

	@Override
	public String toString() {
		return "SetOptions [format=" + format + ", field=" + field + ", root=" + root + "]";
	}

	public void setField(String field) {
		this.field = Optional.of(field);
	}

	public String getRoot() {
		return root;
	}

	public void setRoot(String root) {
		this.root = root;
	}

}
