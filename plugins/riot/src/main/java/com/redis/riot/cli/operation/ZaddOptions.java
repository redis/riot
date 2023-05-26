package com.redis.riot.cli.operation;

import java.util.Optional;

import picocli.CommandLine.Option;

public class ZaddOptions {

	public static final double DEFAULT_SCORE = 1;

	@Option(names = "--score", description = "Name of the field to use for scores.", paramLabel = "<field>")
	private Optional<String> score = Optional.empty();
	@Option(names = "--score-default", description = "Score when field not present (default: ${DEFAULT-VALUE}).", paramLabel = "<num>")
	private double defaultScore = DEFAULT_SCORE;

	public Optional<String> getScore() {
		return score;
	}

	public void setScore(Optional<String> scoreField) {
		this.score = scoreField;
	}

	@Override
	public String toString() {
		return "ZaddOptions [scoreField=" + score + ", scoreDefault=" + defaultScore + "]";
	}

	public double getDefaultScore() {
		return defaultScore;
	}

	public void setDefaultScore(double scoreDefault) {
		this.defaultScore = scoreDefault;
	}

}
