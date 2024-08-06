package com.redis.riot.operation;

import java.util.Map;
import java.util.function.Function;

import com.redis.lettucemod.search.Suggestion;
import com.redis.riot.function.ToSuggestion;
import com.redis.spring.batch.item.redis.writer.impl.Sugadd;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "ft.sugadd", description = "Add suggestion strings to a RediSearch auto-complete dictionary")
public class SugaddCommand extends AbstractOperationCommand {

	@Option(names = "--value", required = true, description = "Field containing the suggestion to add.", paramLabel = "<field>")
	private String stringField;

	@Option(names = "--payload", description = "Field containing the payload.", paramLabel = "<field>")
	private String payloadField;

	@Option(names = "--increment", description = "Increment the existing suggestion by the score instead of replacing the score.")
	private boolean increment;

	@ArgGroup(exclusive = false)
	private ScoreArgs scoreArgs = new ScoreArgs();

	@Override
	public Sugadd<String, String, Map<String, Object>> operation() {
		Sugadd<String, String, Map<String, Object>> operation = new Sugadd<>(keyFunction(), suggestion());
		operation.setIncr(increment);
		return operation;
	}

	private Function<Map<String, Object>, Suggestion<String>> suggestion() {
		return new ToSuggestion<>(toString(stringField), score(scoreArgs), toString(payloadField));
	}

	public String getStringField() {
		return stringField;
	}

	public void setStringField(String string) {
		this.stringField = string;
	}

	public String getPayloadField() {
		return payloadField;
	}

	public void setPayloadField(String payload) {
		this.payloadField = payload;
	}

	public boolean isIncrement() {
		return increment;
	}

	public void setIncrement(boolean increment) {
		this.increment = increment;
	}

	public ScoreArgs getScoreArgs() {
		return scoreArgs;
	}

	public void setScoreArgs(ScoreArgs scoreArgs) {
		this.scoreArgs = scoreArgs;
	}

}
