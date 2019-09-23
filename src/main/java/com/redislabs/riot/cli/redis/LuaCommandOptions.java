package com.redislabs.riot.cli.redis;

import com.redislabs.riot.redis.writer.EvalshaMapWriter;

import io.lettuce.core.ScriptOutputType;
import picocli.CommandLine.Option;

public class LuaCommandOptions {

	@Option(names = "--eval-args", arity = "1..*", description = "Arg field names", paramLabel = "<names>")
	private String[] evalArgs = new String[0];
	@Option(names = "--eval-keys", arity = "1..*", description = "Key field names", paramLabel = "<names>")
	private String[] evalKeys = new String[0];
	@Option(names = "--eval-sha", description = "SHA1 digest", paramLabel = "<sha>")
	private String evalSha;
	@Option(names = "--eval-output", description = "Output: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<type>")
	private ScriptOutputType outputType = ScriptOutputType.STATUS;

	public EvalshaMapWriter writer() {
		EvalshaMapWriter luaWriter = new EvalshaMapWriter();
		luaWriter.setArgs(evalArgs);
		luaWriter.setKeys(evalKeys);
		luaWriter.setOutputType(outputType);
		luaWriter.setSha(evalSha);
		return luaWriter;
	}

}
