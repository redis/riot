package com.redislabs.riot.cli.file;

import java.io.IOException;
import java.io.Writer;
import java.util.Arrays;
import java.util.Map;

import org.springframework.batch.item.file.FlatFileHeaderCallback;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.core.io.Resource;

import com.redislabs.picocliredis.RedisOptions;
import com.redislabs.riot.batch.file.FlatResourceItemWriterBuilder;
import com.redislabs.riot.batch.file.JsonResourceItemWriterBuilder;
import com.redislabs.riot.cli.ExportCommand;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(name = "file-export", description = "Export to file")
public class FileExportCommand extends ExportCommand {

	@ArgGroup(exclusive = false, heading = "File reader options%n", order = 3)
	private FileWriterOptions fileWriterOptions = new FileWriterOptions();

	private FlatResourceItemWriterBuilder<Map<String, Object>> flatWriterBuilder(Resource resource, String headerLine) {
		FlatResourceItemWriterBuilder<Map<String, Object>> builder = new FlatResourceItemWriterBuilder<>();
		builder.append(fileWriterOptions.isAppend());
		builder.encoding(fileWriterOptions.getEncoding());
		builder.lineSeparator(fileWriterOptions.getLineSeparator());
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
	protected AbstractItemStreamItemWriter<Map<String, Object>> writer(RedisOptions redisOptions) throws IOException {
		Resource resource = fileWriterOptions.outputResource();
		switch (fileWriterOptions.type()) {
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
		builder.append(fileWriterOptions.isAppend());
		builder.encoding(fileWriterOptions.getEncoding());
		builder.jsonObjectMarshaller(new JacksonJsonObjectMarshaller<>());
		builder.lineSeparator(fileWriterOptions.getLineSeparator());
		builder.resource(resource);
		builder.saveState(false);
		return builder.build();
	}

	private AbstractItemStreamItemWriter<Map<String, Object>> delimitedWriter(Resource resource) {
		String headerLine = null;
		if (fileWriterOptions.isHeader()) {
			headerLine = String.join(fileWriterOptions.getDelimiter(), fileWriterOptions.getNames());
		}
		FlatResourceItemWriterBuilder<Map<String, Object>> builder = flatWriterBuilder(resource, headerLine);
		builder.name("delimited-s3-writer");
		com.redislabs.riot.batch.file.FlatResourceItemWriterBuilder.DelimitedBuilder<Map<String, Object>> delimited = builder
				.delimited();
		delimited.delimiter(fileWriterOptions.getDelimiter());
		delimited.fieldExtractor(new MapFieldExtractor(fileWriterOptions.getNames()));
		if (!fileWriterOptions.getNames().isEmpty()) {
			delimited.names(fileWriterOptions.getNames().toArray(new String[fileWriterOptions.getNames().size()]));
		}
		return builder.build();
	}

	private AbstractItemStreamItemWriter<Map<String, Object>> formattedWriter(Resource resource) {
		String headerLine = null;
		if (fileWriterOptions.isHeader()) {
			headerLine = String.format(fileWriterOptions.getLocale(), fileWriterOptions.getFormat(),
					Arrays.asList(fileWriterOptions.getNames()).toArray());
		}
		FlatResourceItemWriterBuilder<Map<String, Object>> builder = flatWriterBuilder(resource, headerLine);
		FlatResourceItemWriterBuilder.FormattedBuilder<Map<String, Object>> formatted = builder.formatted();
		builder.name("formatted-s3-writer");
		formatted.fieldExtractor(new MapFieldExtractor(fileWriterOptions.getNames()));
		if (!fileWriterOptions.getNames().isEmpty()) {
			formatted.names(fileWriterOptions.getNames().toArray(new String[fileWriterOptions.getNames().size()]));
		}
		formatted.format(fileWriterOptions.getFormat());
		formatted.locale(fileWriterOptions.getLocale());
		if (fileWriterOptions.getMinLength() != null) {
			formatted.minimumLength(fileWriterOptions.getMinLength());
		}
		if (fileWriterOptions.getMaxLength() != null) {
			formatted.maximumLength(fileWriterOptions.getMaxLength());
		}
		return builder.build();
	}

}
