package com.redislabs.riot.redis;

import java.util.Map;

import org.springframework.vault.support.JsonMapFlattener;

import picocli.CommandLine.Command;

@Command(name = "hmset")
public class HmsetCommand extends AbstractKeyCommand {

	@Override
	protected AbstractKeyWriter<String, String, Map<String, Object>> keyWriter() {
		Hmset<String, String, Map<String, Object>> writer = new Hmset<>();
		writer.setMapConverter(JsonMapFlattener::flattenToStringMap);
		return writer;
	}

}
