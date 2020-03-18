package com.redislabs.riot.cli;

import com.redislabs.riot.redis.writer.map.Evalsha;

import io.lettuce.core.ScriptOutputType;
import picocli.CommandLine.Option;

public class EvalshaOptions {

	@Option(names = "--args", arity = "1..*", description = "EVALSHA arg field names", paramLabel = "<fields>")
	private String[] args = new String[0];
	@Option(names = "--sha", description = "EVALSHA digest", paramLabel = "<sha>")
	private String sha;
	@Option(names = "--output", description = "EVALSHA output: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<type>")
	private ScriptOutputType outputType = ScriptOutputType.STATUS;

	public Evalsha evalsha() {
		return new Evalsha().args(args).outputType(outputType).sha(sha);
	}

}
