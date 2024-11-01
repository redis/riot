package com.redis.riot.operation;

import lombok.ToString;
import picocli.CommandLine.Option;

@ToString
public class ScoreArgs {

	public static final double DEFAULT_SCORE = 1;

	@Option(names = "--score", description = "Name of the field to use for scores.", paramLabel = "<field>")
	private String field;

	@Option(names = "--score-default", description = "Score when field not present (default: ${DEFAULT-VALUE}).", paramLabel = "<num>")
	private double defaultValue = DEFAULT_SCORE;

	public String getField() {
		return field;
	}

	public void setField(String field) {
		this.field = field;
	}

	public double getDefaultValue() {
		return defaultValue;
	}

	public void setDefaultValue(double defaultValue) {
		this.defaultValue = defaultValue;
	}

}
