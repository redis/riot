package com.redislabs.riot.redis;

import java.util.Map;

import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "expire")
public class ExpireCommand extends AbstractKeyCommand {

	@CommandLine.Option(names = "--ttl", description = "EXPIRE timeout field", paramLabel = "<field>")
	private String timeoutField;
	@CommandLine.Option(names = "--ttl-default", defaultValue = "60", description = "EXPIRE default timeout (default: ${DEFAULT-VALUE})", paramLabel = "<sec>")
	private long timeoutDefault;

	@Override
	protected AbstractKeyWriter<String, String, Map<String, Object>> keyWriter() {
		Expire<String, String, Map<String, Object>> writer = new Expire<>();
		writer.setTimeoutConverter(numberFieldExtractor(Long.class, timeoutField, timeoutDefault));
		return writer;
	}

}
