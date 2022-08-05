package com.redis.riot.redis;

import java.util.Arrays;

import io.lettuce.core.ScriptOutputType;
import picocli.CommandLine.Option;

public class EvalOptions {
	@Option(names = "--sha", description = "Digest", paramLabel = "<sha>")
	private String sha;
	@Option(arity = "1..*", names = "--keys", description = "Key fields", paramLabel = "<names>")
	private String[] keys;
	@Option(arity = "1..*", names = "--args", description = "Arg fields", paramLabel = "<names>")
	private String[] args;
	@Option(names = "--output", description = "Output: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<type>")
	private ScriptOutputType outputType = ScriptOutputType.STATUS;

	public String getSha() {
		return sha;
	}

	public void setSha(String sha) {
		this.sha = sha;
	}

	public String[] getKeys() {
		return keys;
	}

	public void setKeys(String[] keys) {
		this.keys = keys;
	}

	public String[] getArgs() {
		return args;
	}

	public void setArgs(String[] args) {
		this.args = args;
	}

	public ScriptOutputType getOutputType() {
		return outputType;
	}

	public void setOutputType(ScriptOutputType outputType) {
		this.outputType = outputType;
	}

	@Override
	public String toString() {
		return "EvalOptions [sha=" + sha + ", keys=" + Arrays.toString(keys) + ", args=" + Arrays.toString(args)
				+ ", outputType=" + outputType + "]";
	}

}
