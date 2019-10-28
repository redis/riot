package com.redislabs.riot.cli;

import java.util.Map;

import org.springframework.batch.item.ItemWriter;

import com.redislabs.riot.batch.ConsoleWriter;
import com.redislabs.riot.cli.redis.RedisConnectionOptions;

import picocli.CommandLine.Command;

@Command(name = "console-export", description = "Export to console")
public class ConsoleExportCommand extends ExportCommand {

	@Override
	protected ItemWriter<Map<String, Object>> writer(RedisConnectionOptions options) throws Exception {
		return new ConsoleWriter();
	}

}
