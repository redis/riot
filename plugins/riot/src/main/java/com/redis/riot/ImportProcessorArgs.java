package com.redis.riot;

import com.redis.riot.core.EvaluationContextArgs;
import com.redis.riot.core.MapProcessorArgs;

import picocli.CommandLine.ArgGroup;

public class ImportProcessorArgs {

	@ArgGroup(exclusive = false)
	private EvaluationContextArgs evaluationContextArgs = new EvaluationContextArgs();

	@ArgGroup(exclusive = false)
	private MapProcessorArgs mapProcessorArgs = new MapProcessorArgs();

	public EvaluationContextArgs getEvaluationContextArgs() {
		return evaluationContextArgs;
	}

	public void setEvaluationContextArgs(EvaluationContextArgs evaluationContextArgs) {
		this.evaluationContextArgs = evaluationContextArgs;
	}

	public MapProcessorArgs getMapProcessorArgs() {
		return mapProcessorArgs;
	}

	public void setMapProcessorArgs(MapProcessorArgs mapProcessorArgs) {
		this.mapProcessorArgs = mapProcessorArgs;
	}

	@Override
	public String toString() {
		return "ImportProcessorArgs [evaluationContextArgs=" + evaluationContextArgs + ", mapProcessorArgs="
				+ mapProcessorArgs + "]";
	}

}
