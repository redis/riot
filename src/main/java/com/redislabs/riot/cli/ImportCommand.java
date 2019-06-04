package com.redislabs.riot.cli;

import java.util.Map;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStreamWriter;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;

import com.redislabs.riot.RiotApplication;

import lombok.Getter;
import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

@Command(name = "import", description = "Import into Redis", subcommands = { DelimitedImportSubCommand.class,
		FixedLengthImportSubCommand.class, JsonImportSubCommand.class, DatabaseImportSubCommand.class,
		GeneratorImportSubCommand.class, SimpleGeneratorImportSubCommand.class })
public class ImportCommand extends BaseCommand {

	@ParentCommand
	@Getter
	private RiotApplication parent;

	public ExitStatus call(AbstractItemCountingItemStreamItemReader<Map<String, Object>> reader,
			ItemProcessor<Map<String, Object>, Map<String, Object>> processor,
			ItemStreamWriter<Map<String, Object>> writer) throws Exception {
		return parent.call(reader, processor, writer);
	}

}
