package com.redislabs.riot.cli.redis;

import com.redislabs.riot.redis.writer.LuaWriter;

import io.lettuce.core.ScriptOutputType;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "lua", description = "Redis Lua script")
public class LuaWriterCommand extends AbstractDataStructureWriterCommand {

	@Option(names = "--sha", description = "SHA1 digest of the LUA script")
	private String sha;
	@Option(names = "--output", description = "Script output type: ${COMPLETION-CANDIDATES}", paramLabel = "<type>")
	private ScriptOutputType outputType = ScriptOutputType.STATUS;
	@Option(names = "--lua-keys", arity = "1..*", description = "Field names of the LUA script keys", paramLabel = "<field1,field2,...>")
	private String[] keys = new String[0];
	@Option(names = "--lua-args", arity = "1..*", description = "Field names of the LUA script args", paramLabel = "<field1,field2,...>")
	private String[] args = new String[0];

	protected LuaWriter writer() {
		LuaWriter writer = new LuaWriter(sha);
		writer.setOutputType(outputType);
		writer.setKeys(keys);
		writer.setArgs(args);
		return writer;
	}
}