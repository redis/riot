package com.redis.riot.cli.redis;

import com.redis.riot.core.operation.SugaddBuilder;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "ft.sugadd", description = "Add suggestion strings to a RediSearch auto-complete dictionary")
public class SugaddCommand extends AbstractWriteOperationCommand {

	public static final double DEFAULT_SCORE = 1;

	public static final boolean DEFAULT_INCREMENT = false;

	@Option(names = "--field", required = true, description = "Field containing the strings to add.", paramLabel = "<field>")
	private String stringField;

	@Option(names = "--score", description = "Name of the field to use for scores.", paramLabel = "<field>")
	private String scoreField;

	@Option(names = "--score-default", description = "Score when field not present (default: ${DEFAULT-VALUE}).", paramLabel = "<num>")
	private double defaultScore = DEFAULT_SCORE;

	@Option(names = "--payload", description = "Field containing the payload.", paramLabel = "<field>")
	private String payloadField;

	@Option(names = "--increment", description = "Increment the existing suggestion by the score instead of replacing the score.")
	private boolean increment = DEFAULT_INCREMENT;

	public String getStringField() {
		return stringField;
	}

	public void setStringField(String field) {
		this.stringField = field;
	}

	public String getScoreField() {
		return scoreField;
	}

	public void setScore(String field) {
		this.scoreField = field;
	}

	public double getDefaultScore() {
		return defaultScore;
	}

	public void setDefaultScore(double scoreDefault) {
		this.defaultScore = scoreDefault;
	}

	public String getPayloadField() {
		return payloadField;
	}

	public void setPayload(String field) {
		this.payloadField = field;
	}

	public boolean isIncrement() {
		return increment;
	}

	public void setIncrement(boolean increment) {
		this.increment = increment;
	}

	@Override
	protected SugaddBuilder operationBuilder() {
		SugaddBuilder supplier = new SugaddBuilder();
		supplier.setDefaultScore(defaultScore);
		supplier.setIncrement(increment);
		supplier.setStringField(stringField);
		supplier.setPayloadField(payloadField);
		supplier.setScoreField(scoreField);
		return supplier;
	}

}
