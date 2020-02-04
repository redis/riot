package com.redislabs.riot.cli.file;

import java.util.Map;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.file.FlatFileParseException;

import com.redislabs.riot.cli.MapImportCommand;
import com.redislabs.riot.transfer.ErrorHandler;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Slf4j
@Command(name = "file-import", description = "Import from a file")
public @Data class FileImportCommand extends MapImportCommand {

	@ArgGroup(exclusive = false, heading = "File options%n", order = 3)
	private FileReaderOptions options = new FileReaderOptions();

	@Override
	protected ItemReader<Map<String, Object>> reader() throws Exception {
		return options.reader();
	}

	@Override
	protected ItemProcessor<Map<String, Object>, Map<String, Object>> postProcessor() {
		return options.postProcessor();
	}

	@Override
	protected ErrorHandler errorHandler() {
		ErrorHandler errorHandler = super.errorHandler();
		return e -> {
			if (e instanceof FlatFileParseException) {
				FlatFileParseException parseException = (FlatFileParseException) e;
				log.error("Could not parse line #{}: '{}'", parseException.getLineNumber(), parseException.getInput(),
						e);
			} else {
				errorHandler.handle(e);
			}
		};
	}

}
