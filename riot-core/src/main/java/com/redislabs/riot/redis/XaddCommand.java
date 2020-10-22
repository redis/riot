package com.redislabs.riot.redis;

import java.util.Map;

import org.springframework.vault.support.JsonMapFlattener;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "xadd")
public class XaddCommand extends AbstractKeyCommand {

	@Option(names = "--id", description = "Stream entry ID field", paramLabel = "<field>")
	private String idField;
	@Option(names = "--maxlen", description = "Stream maxlen", paramLabel = "<int>")
	private Long maxlen;
	@Option(names = "--trim", description = "Stream efficient trimming ('~' flag)")
	private boolean approximateTrimming;

	@Override
	protected AbstractKeyWriter<String, String, Map<String, Object>> keyWriter() {
		Xadd<String, String, Map<String, Object>> writer = new Xadd<>();
		writer.setApproximateTrimming(approximateTrimming);
		writer.setMaxlen(maxlen);
		writer.setIdConverter(stringFieldExtractor(idField));
		writer.setBodyConverter(JsonMapFlattener::flattenToStringMap);
		return writer;
	}

}
