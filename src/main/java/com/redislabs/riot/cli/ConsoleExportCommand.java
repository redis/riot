package com.redislabs.riot.cli;

import java.util.Map;

import org.springframework.batch.item.ItemWriter;

import com.redislabs.picocliredis.RedisOptions;
import com.redislabs.riot.batch.ConsoleWriter;

import picocli.CommandLine.Command;

@Command(name = "console-export", description = "Export to console")
public class ConsoleExportCommand extends ExportCommand {

	@Override
	protected ItemWriter<Map<String, Object>> writer(RedisOptions options) throws Exception {
		return new ConsoleWriter();
	}

}
