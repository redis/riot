package com.redislabs.riot.cli;

import java.util.Map;

import org.springframework.batch.item.ItemReader;

import com.redislabs.riot.cli.db.DatabaseImportCommand;
import com.redislabs.riot.cli.file.FileImportCommand;
import com.redislabs.riot.cli.generator.FakerGeneratorCommand;
import com.redislabs.riot.cli.generator.SimpleGeneratorCommand;
import com.redislabs.riot.cli.redis.RedisWriterOptions;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(name = "import", description = "Import data into Redis", subcommands = { FileImportCommand.class,
		DatabaseImportCommand.class, FakerGeneratorCommand.class,
		SimpleGeneratorCommand.class }, synopsisSubcommandLabel = "[CONNECTOR]", commandListHeading = "Connectors:%n")
public class ImportParentCommand extends TransferCommand {

	@ArgGroup(exclusive = false, heading = "Redis writer options%n")
	private RedisWriterOptions redisWriter = new RedisWriterOptions();

	public void transfer(ItemReader<Map<String, Object>> reader) {
		transfer("import", reader, redisWriter.writer(getRiot().getRedis()));
	}

}
