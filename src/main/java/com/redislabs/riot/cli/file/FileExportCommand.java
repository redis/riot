package com.redislabs.riot.cli.file;

import java.io.IOException;
import java.io.Writer;
import java.util.Arrays;
import java.util.Map;

import org.springframework.batch.item.file.FlatFileHeaderCallback;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.core.io.Resource;

import com.redislabs.riot.cli.ExportCommand;
import com.redislabs.riot.file.FlatResourceItemWriterBuilder;
import com.redislabs.riot.file.JsonResourceItemWriterBuilder;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(name = "file-export", description = "Export to file")
public class FileExportCommand extends ExportCommand {

	@ArgGroup(exclusive = false, heading = "File options%n", order = 1)
	private FileOptions fileOptions;
	@ArgGroup(exclusive = false, heading = "File reader options%n", order = 3)
	private FileReaderOptions fileReaderOptions = new FileReaderOptions();

	private FlatResourceItemWriterBuilder<Map<String, Object>> flatWriterBuilder(Resource resource, String headerLine) {
		FlatResourceItemWriterBuilder<Map<String, Object>> builder = new FlatResourceItemWriterBuilder<>();
		builder.append(fileReaderOptions.isAppend());
		builder.encoding(fileOptions.getEncoding());
		builder.lineSeparator(fileReaderOptions.getLineSeparator());
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

	@Override
	protected AbstractItemStreamItemWriter<Map<String, Object>> writer() throws IOException {
		Resource resource = fileOptions.outputResource();
		switch (fileOptions.type()) {
		case json:
			return jsonWriter(resource);
		case fixed:
			return formattedWriter(resource);
		default:
			return delimitedWriter(resource);
		}
	}

	private AbstractItemStreamItemWriter<Map<String, Object>> jsonWriter(Resource resource) {
		JsonResourceItemWriterBuilder<Map<String, Object>> builder = new JsonResourceItemWriterBuilder<>();
		builder.name("json-s3-writer");
		builder.append(fileReaderOptions.isAppend());
		builder.encoding(fileOptions.getEncoding());
		builder.jsonObjectMarshaller(new JacksonJsonObjectMarshaller<>());
		builder.lineSeparator(fileReaderOptions.getLineSeparator());
		builder.resource(resource);
		builder.saveState(false);
		return builder.build();
	}

	private AbstractItemStreamItemWriter<Map<String, Object>> delimitedWriter(Resource resource) {
		String headerLine = null;
		if (fileOptions.isHeader()) {
			headerLine = String.join(fileOptions.getDelimiter(), fileReaderOptions.getNames());
		}
		FlatResourceItemWriterBuilder<Map<String, Object>> builder = flatWriterBuilder(resource, headerLine);
		builder.name("delimited-s3-writer");
		com.redislabs.riot.file.FlatResourceItemWriterBuilder.DelimitedBuilder<Map<String, Object>> delimited = builder
				.delimited();
		delimited.delimiter(fileOptions.getDelimiter());
		delimited.fieldExtractor(new MapFieldExtractor(fileReaderOptions.getNames()));
		if (fileReaderOptions.getNames().length > 0) {
			delimited.names(fileReaderOptions.getNames());
		}
		return builder.build();
	}

	private AbstractItemStreamItemWriter<Map<String, Object>> formattedWriter(Resource resource) {
		String headerLine = null;
		if (fileOptions.isHeader()) {
			headerLine = String.format(fileReaderOptions.getLocale(), fileReaderOptions.getFormat(),
					Arrays.asList(fileReaderOptions.getNames()).toArray());
		}
		FlatResourceItemWriterBuilder<Map<String, Object>> builder = flatWriterBuilder(resource, headerLine);
		FlatResourceItemWriterBuilder.FormattedBuilder<Map<String, Object>> formatted = builder.formatted();
		builder.name("formatted-s3-writer");
		formatted.fieldExtractor(new MapFieldExtractor(fileReaderOptions.getNames()));
		if (fileReaderOptions.getNames().length > 0) {
			formatted.names(fileReaderOptions.getNames());
		}
		formatted.format(fileReaderOptions.getFormat());
		formatted.locale(fileReaderOptions.getLocale());
		if (fileReaderOptions.getMinLength() != null) {
			formatted.minimumLength(fileReaderOptions.getMinLength());
		}
		if (fileReaderOptions.getMaxLength() != null) {
			formatted.maximumLength(fileReaderOptions.getMaxLength());
		}
		return builder.build();
	}

}
