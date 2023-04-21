package com.redis.riot.cli.operation;

import java.util.Optional;

import picocli.CommandLine.Option;

public class SugaddOptions {

	@Option(names = "--field", required = true, description = "Field containing the strings to add.", paramLabel = "<field>")
	private String field;
	@Option(names = "--score", description = "Name of the field to use for scores.", paramLabel = "<field>")
	private Optional<String> scoreField = Optional.empty();
	@Option(names = "--score-default", description = "Score when field not present (default: ${DEFAULT-VALUE}).", paramLabel = "<num>")
	private double scoreDefault = 1;
	@Option(names = "--payload", description = "Field containing the payload.", paramLabel = "<field>")
	private Optional<String> payload = Optional.empty();
	@Option(names = "--increment", description = "Increment the existing suggestion by the score instead of replacing the score.")
	private boolean increment;

	public String getField() {
		return field;
	}

	public void setField(String field) {
		this.field = field;
	}

	public Optional<String> getScoreField() {
		return scoreField;
	}

	public void setScoreField(Optional<String> scoreField) {
		this.scoreField = scoreField;
	}

	public double getScoreDefault() {
		return scoreDefault;
	}

	public void setScoreDefault(double scoreDefault) {
		this.scoreDefault = scoreDefault;
	}

	public Optional<String> getPayload() {
		return payload;
	}

	public void setPayload(Optional<String> payload) {
		this.payload = payload;
	}

	public boolean isIncrement() {
		return increment;
	}

	public void setIncrement(boolean increment) {
		this.increment = increment;
	}

	@Override
	public String toString() {
		return "SugaddOptions [field=" + field + ", scoreField=" + scoreField + ", scoreDefault=" + scoreDefault
				+ ", payload=" + payload + ", increment=" + increment + "]";
	}

}
