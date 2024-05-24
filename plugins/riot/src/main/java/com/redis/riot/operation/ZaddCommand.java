package com.redis.riot.operation;

import java.util.Arrays;
import java.util.Map;

import com.redis.riot.function.ToScoredValueFunction;
import com.redis.spring.batch.item.redis.writer.operation.Zadd;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(name = "zadd", description = "Add members with scores to a sorted set")
public class ZaddCommand extends AbstractMemberOperationCommand {

	@ArgGroup(exclusive = false)
	private ScoreArgs scoreArgs = new ScoreArgs();

	@Override
	public Zadd<String, String, Map<String, Object>> operation() {
		return new Zadd<>(keyFunction(), scoredValueFunction().andThen(Arrays::asList));
	}

	private ToScoredValueFunction<String, Map<String, Object>> scoredValueFunction() {
		return new ToScoredValueFunction<>(memberFunction(), score(scoreArgs));
	}

	public ScoreArgs getScoreArgs() {
		return scoreArgs;
	}

	public void setScoreArgs(ScoreArgs scoreArgs) {
		this.scoreArgs = scoreArgs;
	}

}