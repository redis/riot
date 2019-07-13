package com.redislabs.riot.cli.file;

import java.util.Map;

import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.batch.item.json.JsonFileItemWriter;
import org.springframework.batch.item.json.builder.JsonFileItemWriterBuilder;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "json", description = "JSON file")
public class JsonFileWriterCommand extends AbstractFileWriterCommand {

	@Option(names = "--separator", description = "String used to separate lines in output.", paramLabel = "<string>")
	private String lineSeparator = JsonFileItemWriter.DEFAULT_LINE_SEPARATOR;

	@Override
	protected ItemWriter<Map<String, Object>> writer() {
		JsonFileItemWriterBuilder<Map<String, Object>> builder = new JsonFileItemWriterBuilder<>();
		builder.name("json-file-writer");
		builder.append(append);
		builder.encoding(encoding);
		builder.forceSync(forceSync);
		builder.jsonObjectMarshaller(new JacksonJsonObjectMarshaller<>());
		builder.lineSeparator(lineSeparator);
		builder.resource(resource());
		builder.saveState(false);
		return builder.build();
	}

}
