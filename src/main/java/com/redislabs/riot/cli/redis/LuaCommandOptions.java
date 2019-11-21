package com.redislabs.riot.cli.redis;

import java.util.ArrayList;
import java.util.List;

import com.redislabs.riot.batch.redis.map.EvalshaMapWriter;

import io.lettuce.core.ScriptOutputType;
import lombok.Data;
import picocli.CommandLine.Option;

public @Data class LuaCommandOptions {

	@Option(names = "--eval-args", arity = "1..*", description = "Arg field names", paramLabel = "<names>")
	private List<String> evalArgs = new ArrayList<>();
	@Option(names = "--eval-keys", arity = "1..*", description = "Key field names", paramLabel = "<names>")
	private List<String> evalKeys = new ArrayList<>();
	@Option(names = "--eval-sha", description = "SHA1 digest", paramLabel = "<sha>")
	private String evalSha;
	@Option(names = "--eval-output", description = "Output: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<type>")
	private ScriptOutputType outputType = ScriptOutputType.STATUS;

	public <R> EvalshaMapWriter<R> writer() {
		return new EvalshaMapWriter<R>().args(evalArgs).keys(evalKeys).outputType(outputType).sha(evalSha);
	}

}
