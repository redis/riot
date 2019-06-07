package com.redislabs.riot.cli.out.file;

import java.util.Map;

import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.batch.item.json.JsonFileItemWriter;
import org.springframework.batch.item.json.builder.JsonFileItemWriterBuilder;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "json", description = "Export to a JSON file")
public class JsonFileExport extends AbstractFileExport {

	@Option(names = "--line-separator", description = "String used to separate lines in output. (default: ${DEFAULT-VALUE}).")
	private String lineSeparator = JsonFileItemWriter.DEFAULT_LINE_SEPARATOR;

	@Override
	protected ItemWriter<Map<String, Object>> writer() {
		JsonFileItemWriterBuilder<Map<String, Object>> builder = new JsonFileItemWriterBuilder<>();
		builder.name("json-file-writer");
		builder.append(isAppend());
		builder.encoding(getEncoding());
		builder.forceSync(isForceSync());
		builder.jsonObjectMarshaller(new JacksonJsonObjectMarshaller<>());
		builder.lineSeparator(lineSeparator);
		builder.resource(resource());
		builder.saveState(false);
		return builder.build();
	}

}
