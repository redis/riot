package com.redis.riot.cli.operation;

import java.util.Optional;

import picocli.CommandLine.Option;

public class SugaddOptions {

	public static final double DEFAULT_SCORE = 1;
	public static final boolean DEFAULT_INCREMENT = false;

	@Option(names = "--field", required = true, description = "Field containing the strings to add.", paramLabel = "<field>")
	private String field;

	@Option(names = "--score", description = "Name of the field to use for scores.", paramLabel = "<field>")
	private Optional<String> score = Optional.empty();

	@Option(names = "--score-default", description = "Score when field not present (default: ${DEFAULT-VALUE}).", paramLabel = "<num>")
	private double defaultScore = DEFAULT_SCORE;

	@Option(names = "--payload", description = "Field containing the payload.", paramLabel = "<field>")
	private Optional<String> payload = Optional.empty();

	@Option(names = "--increment", description = "Increment the existing suggestion by the score instead of replacing the score.")
	private boolean increment = DEFAULT_INCREMENT;

	public String getField() {
		return field;
	}

	public void setField(String field) {
		this.field = field;
	}

	public Optional<String> getScore() {
		return score;
	}

	public void setScore(Optional<String> scoreField) {
		this.score = scoreField;
	}

	public double getDefaultScore() {
		return defaultScore;
	}

	public void setDefaultScore(double scoreDefault) {
		this.defaultScore = scoreDefault;
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
		return "SugaddOptions [field=" + field + ", scoreField=" + score + ", scoreDefault=" + defaultScore
				+ ", payload=" + payload + ", increment=" + increment + "]";
	}

}
