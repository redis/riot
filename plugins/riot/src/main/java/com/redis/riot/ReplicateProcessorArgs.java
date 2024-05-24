package com.redis.riot;

import com.redis.riot.core.EvaluationContextArgs;

import picocli.CommandLine.ArgGroup;

public class ReplicateProcessorArgs {

	@ArgGroup(exclusive = false)
	private EvaluationContextArgs evaluationContextArgs = new EvaluationContextArgs();

	@ArgGroup(exclusive = false)
	private KeyValueProcessorArgs keyValueProcessorArgs = new KeyValueProcessorArgs();

	public EvaluationContextArgs getEvaluationContextArgs() {
		return evaluationContextArgs;
	}

	public void setEvaluationContextArgs(EvaluationContextArgs args) {
		this.evaluationContextArgs = args;
	}

	public KeyValueProcessorArgs getKeyValueProcessorArgs() {
		return keyValueProcessorArgs;
	}

	public void setKeyValueProcessorArgs(KeyValueProcessorArgs args) {
		this.keyValueProcessorArgs = args;
	}

}
