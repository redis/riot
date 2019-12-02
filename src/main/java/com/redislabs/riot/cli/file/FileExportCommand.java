package com.redislabs.riot.cli.file;

import java.io.IOException;
import java.io.Writer;
import java.util.Arrays;
import java.util.Map;

import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileHeaderCallback;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.core.io.Resource;

import com.redislabs.riot.batch.file.FlatResourceItemWriterBuilder;
import com.redislabs.riot.batch.file.JsonResourceItemWriterBuilder;
import com.redislabs.riot.cli.HashExportCommand;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(name = "file-export", description = "Export to file")
public class FileExportCommand extends HashExportCommand {

	@ArgGroup(exclusive = false, heading = "File options%n", order = 3)
	private FileWriterOptions options = new FileWriterOptions();

	protected ItemWriter<Map<String, Object>> writer() throws Exception {
		Resource resource = options.outputResource();
		switch (options.type()) {
		case Json:
			return jsonWriter(resource);
		case Fixed:
			return formattedWriter(resource);
		default:
			return delimitedWriter(resource);
		}
	}

	private FlatResourceItemWriterBuilder<Map<String, Object>> flatWriterBuilder(Resource resource, String headerLine) {
		FlatResourceItemWriterBuilder<Map<String, Object>> builder = new FlatResourceItemWriterBuilder<>();
		builder.append(options.append());
		builder.encoding(options.encoding());
		builder.lineSeparator(options.lineSeparator());
		builder.resource(resource);
		builder.saveState(false);
		if (headerLine != null) {
			builder.headerCallback(new FlatFileHeaderCallback() {

				@Override
				public void writeHeader(Writer writer) throws IOException {
					writer.write(headerLine);
				}
			});
		}
		return builder;
	}

	private AbstractItemStreamItemWriter<Map<String, Object>> jsonWriter(Resource resource) {
		JsonResourceItemWriterBuilder<Map<String, Object>> builder = new JsonResourceItemWriterBuilder<>();
		builder.name("json-s3-writer");
		builder.append(options.append());
		builder.encoding(options.encoding());
		builder.jsonObjectMarshaller(new JacksonJsonObjectMarshaller<>());
		builder.lineSeparator(options.lineSeparator());
		builder.resource(resource);
		builder.saveState(false);
		return builder.build();
	}

	private AbstractItemStreamItemWriter<Map<String, Object>> delimitedWriter(Resource resource) {
		String headerLine = null;
		if (options.header()) {
			headerLine = String.join(options.delimiter(), options.names());
		}
		FlatResourceItemWriterBuilder<Map<String, Object>> builder = flatWriterBuilder(resource, headerLine);
		builder.name("delimited-s3-writer");
		com.redislabs.riot.batch.file.FlatResourceItemWriterBuilder.DelimitedBuilder<Map<String, Object>> delimited = builder
				.delimited();
		delimited.delimiter(options.delimiter());
		delimited.fieldExtractor(new MapFieldExtractor(options.names()));
		if (options.names().length > 0) {
			delimited.names(options.names());
		}
		return builder.build();
	}

	private AbstractItemStreamItemWriter<Map<String, Object>> formattedWriter(Resource resource) {
		String headerLine = null;
		if (options.header()) {
			headerLine = String.format(options.locale(), options.format(), Arrays.asList(options.names()).toArray());
		}
		FlatResourceItemWriterBuilder<Map<String, Object>> builder = flatWriterBuilder(resource, headerLine);
		FlatResourceItemWriterBuilder.FormattedBuilder<Map<String, Object>> formatted = builder.formatted();
		builder.name("formatted-s3-writer");
		formatted.fieldExtractor(new MapFieldExtractor(options.names()));
		if (options.names().length > 0) {
			formatted.names(options.names());
		}
		formatted.format(options.format());
		formatted.locale(options.locale());
		if (options.minLength() != null) {
			formatted.minimumLength(options.minLength());
		}
		if (options.maxLength() != null) {
			formatted.maximumLength(options.maxLength());
		}
		return builder.build();
	}

}
