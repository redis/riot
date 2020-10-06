package com.redislabs.riot.redis;

import java.util.Map;

import com.redislabs.riot.convert.ObjectMapToStringArrayConverter;

import io.lettuce.core.ScriptOutputType;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "evalsha")
public class EvalshaCommand extends AbstractRedisCommand<Map<String, Object>> {

	@CommandLine.Option(names = "--sha", description = "Digest", paramLabel = "<sha>")
	private String sha;
	@CommandLine.Option(names = "--keys", arity = "1..*", description = "Key fields", paramLabel = "<names>")
	private String[] keys = new String[0];
	@CommandLine.Option(names = "--args", arity = "1..*", description = "Arg fields", paramLabel = "<names>")
	private String[] args = new String[0];
	@CommandLine.Option(names = "--output", defaultValue = "STATUS", description = "Output: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<type>")
	private ScriptOutputType outputType;

	@Override
	public Evalsha<String, String, Map<String, Object>> writer() {
		Evalsha<String, String, Map<String, Object>> writer = new Evalsha<>();
		writer.setSha(sha);
		writer.setKeysConverter(ObjectMapToStringArrayConverter.builder().fields(keys).build());
		writer.setArgsConverter(ObjectMapToStringArrayConverter.builder().fields(args).build());
		writer.setOutputType(outputType);
		return writer;
	}

}
