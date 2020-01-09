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

import com.redislabs.riot.cli.HashExportCommand;
import com.redislabs.riot.file.FlatResourceItemWriterBuilder;
import com.redislabs.riot.file.JsonResourceItemWriterBuilder;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(name = "file-export", description = "Export to a file")
public class FileExportCommand extends HashExportCommand {

	@ArgGroup(exclusive = false, heading = "File options%n", order = 3)
	private FileWriterOptions options = new FileWriterOptions();

	@Override
	protected ItemWriter<Map<String, Object>> writer() throws IOException {
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
		builder.append(options.isAppend());
		builder.encoding(options.getEncoding());
		builder.lineSeparator(options.getLineSeparator());
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
		builder.append(options.isAppend());
		builder.encoding(options.getEncoding());
		builder.jsonObjectMarshaller(new JacksonJsonObjectMarshaller<>());
		builder.lineSeparator(options.getLineSeparator());
		builder.resource(resource);
		builder.saveState(false);
		return builder.build();
	}

	private AbstractItemStreamItemWriter<Map<String, Object>> delimitedWriter(Resource resource) {
		String headerLine = null;
		if (options.isHeader()) {
			headerLine = String.join(options.getDelimiter(), options.getNames());
		}
		FlatResourceItemWriterBuilder<Map<String, Object>> builder = flatWriterBuilder(resource, headerLine);
		builder.name("delimited-s3-writer");
		com.redislabs.riot.file.FlatResourceItemWriterBuilder.DelimitedBuilder<Map<String, Object>> delimited = builder
				.delimited();
		delimited.delimiter(options.getDelimiter());
		delimited.fieldExtractor(new MapFieldExtractor(options.getNames()));
		if (options.getNames().length > 0) {
			delimited.names(options.getNames());
		}
		return builder.build();
	}

	private AbstractItemStreamItemWriter<Map<String, Object>> formattedWriter(Resource resource) {
		String headerLine = null;
		if (options.isHeader()) {
			headerLine = String.format(options.getLocale(), options.getFormat(),
					Arrays.asList(options.getNames()).toArray());
		}
		FlatResourceItemWriterBuilder<Map<String, Object>> builder = flatWriterBuilder(resource, headerLine);
		FlatResourceItemWriterBuilder.FormattedBuilder<Map<String, Object>> formatted = builder.formatted();
		builder.name("formatted-s3-writer");
		formatted.fieldExtractor(new MapFieldExtractor(options.getNames()));
		if (options.getNames().length > 0) {
			formatted.names(options.getNames());
		}
		formatted.format(options.getFormat());
		formatted.locale(options.getLocale());
		if (options.getMinLength() != null) {
			formatted.minimumLength(options.getMinLength());
		}
		if (options.getMaxLength() != null) {
			formatted.maximumLength(options.getMaxLength());
		}
		return builder.build();
	}

	@Override
	protected String taskName() {
		return "Exporting to " + options.getResourceOptions().path();
	}

}
