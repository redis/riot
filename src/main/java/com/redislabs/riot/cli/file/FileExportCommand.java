package com.redislabs.riot.cli.file;

import java.io.IOException;
import java.io.Writer;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;

import org.springframework.batch.item.file.FlatFileHeaderCallback;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.core.io.Resource;

import com.redislabs.riot.cli.ExportCommand;
import com.redislabs.riot.file.FlatResourceItemWriterBuilder;
import com.redislabs.riot.file.JsonResourceItemWriterBuilder;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

@Command(name = "file-export", description = "Export to file")
public class FileExportCommand extends ExportCommand {

	@Mixin
	private FileOptions connector;
	@Option(names = "--append", description = "Append to file if it exists")
	private boolean append;
	@Option(names = "--force-sync", description = "Force-sync changes to disk on flush")
	private boolean forceSync;
	@Option(names = "--line-separator", description = "String to separate lines (default: system default)", paramLabel = "<string>")
	private String lineSeparator = FlatFileItemWriter.DEFAULT_LINE_SEPARATOR;
	@Option(names = "--format", description = "Format string used to aggregate items", paramLabel = "<string>")
	private String format;
	@Option(names = "--locale", description = "Locale", paramLabel = "<tag>")
	private Locale locale = Locale.ENGLISH;
	@Option(names = "--max-length", description = "Max length of the formatted string", paramLabel = "<int>")
	private Integer maxLength;
	@Option(names = "--min-length", description = "Min length of the formatted string", paramLabel = "<int>")
	private Integer minLength;

	private FlatResourceItemWriterBuilder<Map<String, Object>> flatWriterBuilder(Resource resource, String headerLine) {
		FlatResourceItemWriterBuilder<Map<String, Object>> builder = new FlatResourceItemWriterBuilder<>();
		builder.append(append);
		builder.encoding(connector.getEncoding());
		builder.lineSeparator(lineSeparator);
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
		Resource resource = connector.outputResource();
		switch (connector.type()) {
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
		builder.append(append);
		builder.encoding(connector.getEncoding());
		builder.jsonObjectMarshaller(new JacksonJsonObjectMarshaller<>());
		builder.lineSeparator(lineSeparator);
		builder.resource(resource);
		builder.saveState(false);
		return builder.build();
	}

	private AbstractItemStreamItemWriter<Map<String, Object>> delimitedWriter(Resource resource) {
		String headerLine = null;
		if (connector.isHeader()) {
			headerLine = String.join(connector.getDelimiter(), connector.getNames());
		}
		FlatResourceItemWriterBuilder<Map<String, Object>> builder = flatWriterBuilder(resource, headerLine);
		builder.name("delimited-s3-writer");
		com.redislabs.riot.file.FlatResourceItemWriterBuilder.DelimitedBuilder<Map<String, Object>> delimited = builder
				.delimited();
		delimited.delimiter(connector.getDelimiter());
		delimited.fieldExtractor(new MapFieldExtractor(connector.getNames()));
		if (connector.getNames().length > 0) {
			delimited.names(connector.getNames());
		}
		return builder.build();
	}

	private AbstractItemStreamItemWriter<Map<String, Object>> formattedWriter(Resource resource) {
		String headerLine = null;
		if (connector.isHeader()) {
			headerLine = String.format(locale, format, Arrays.asList(connector.getNames()).toArray());
		}
		FlatResourceItemWriterBuilder<Map<String, Object>> builder = flatWriterBuilder(resource, headerLine);
		FlatResourceItemWriterBuilder.FormattedBuilder<Map<String, Object>> formatted = builder.formatted();
		builder.name("formatted-s3-writer");
		formatted.fieldExtractor(new MapFieldExtractor(connector.getNames()));
		if (connector.getNames().length > 0) {
			formatted.names(connector.getNames());
		}
		formatted.format(format);
		formatted.locale(locale);
		if (minLength != null) {
			formatted.minimumLength(minLength);
		}
		if (maxLength != null) {
			formatted.maximumLength(maxLength);
		}
		return builder.build();
	}

}
