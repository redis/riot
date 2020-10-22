package com.redislabs.riot.redis;

import java.util.Map;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "expire")
public class ExpireCommand extends AbstractKeyCommand {

	@Option(names = "--ttl", description = "EXPIRE timeout field", paramLabel = "<field>")
	private String timeoutField;
	@Option(names = "--ttl-default", description = "EXPIRE default timeout (default: ${DEFAULT-VALUE})", paramLabel = "<sec>")
	private long timeoutDefault = 60;

	@Override
	protected AbstractKeyWriter<String, String, Map<String, Object>> keyWriter() {
		Expire<String, String, Map<String, Object>> writer = new Expire<>();
		writer.setTimeoutConverter(numberFieldExtractor(Long.class, timeoutField, timeoutDefault));
		return writer;
	}

}
