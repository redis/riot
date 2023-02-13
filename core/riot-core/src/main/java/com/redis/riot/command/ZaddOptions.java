package com.redis.riot.command;

import java.util.Optional;

import picocli.CommandLine.Option;

public class ZaddOptions {

	@Option(names = "--score", description = "Name of the field to use for scores.", paramLabel = "<field>")
	private Optional<String> scoreField = Optional.empty();
	@Option(names = "--score-default", description = "Score when field not present (default: ${DEFAULT-VALUE}).", paramLabel = "<num>")
	private double scoreDefault = 1;

	public Optional<String> getScoreField() {
		return scoreField;
	}

	public void setScoreField(Optional<String> scoreField) {
		this.scoreField = scoreField;
	}

	@Override
	public String toString() {
		return "ZaddOptions [scoreField=" + scoreField + ", scoreDefault=" + scoreDefault + "]";
	}

	public double getScoreDefault() {
		return scoreDefault;
	}

	public void setScoreDefault(double scoreDefault) {
		this.scoreDefault = scoreDefault;
	}

}
