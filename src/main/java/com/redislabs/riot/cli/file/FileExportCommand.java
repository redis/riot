package com.redislabs.riot.cli.file;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;

import org.springframework.batch.item.file.FlatFileHeaderCallback;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder.DelimitedBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder.FormattedBuilder;
import org.springframework.batch.item.file.transform.FieldExtractor;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.batch.item.json.JsonFileItemWriter;
import org.springframework.batch.item.json.builder.JsonFileItemWriterBuilder;
import org.springframework.batch.item.support.AbstractFileItemWriter;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

import com.redislabs.riot.cli.ExportCommand;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

@Command(name = "file-export", description = "Export Redis to file")
public class FileExportCommand extends ExportCommand {

	@Option(names = { "-f", "--file" }, description = "Path to file")
	private File path;
	@Mixin
	private FileOptions file = new FileOptions();
	@Option(names = "--append", description = "Append to file if it exists")
	private boolean append;
	@Option(names = "--force-sync", description = "Force-sync changes to disk on flush")
	private boolean forceSync;
	@Option(names = "--line-separator", description = "String to separate lines (default: system default)", paramLabel = "<string>")
	private String lineSeparator = FlatFileItemWriter.DEFAULT_LINE_SEPARATOR;
	@Option(names = "--format", description = "Format string used to aggregate items")
	private String format;
	@Option(names = "--locale", description = "Locale", paramLabel = "<tag>")
	private Locale locale = Locale.ENGLISH;
	@Option(names = "--max-length", description = "Max length of the formatted string", paramLabel = "<int>")
	private Integer maxLength;
	@Option(names = "--min-length", description = "Min length of the formatted string", paramLabel = "<int>")
	private Integer minLength;

	private JsonFileItemWriter<Map<String, Object>> jsonWriter(Resource resource) {
		JsonFileItemWriterBuilder<Map<String, Object>> builder = new JsonFileItemWriterBuilder<>();
		builder.name("json-file-writer");
		builder.append(append);
		builder.encoding(file.getEncoding());
		builder.forceSync(forceSync);
		builder.jsonObjectMarshaller(new JacksonJsonObjectMarshaller<>());
		builder.lineSeparator(lineSeparator);
		builder.resource(resource);
		builder.saveState(false);
		return builder.build();
	}

	private FlatFileItemWriterBuilder<Map<String, Object>> flatFileWriterBuilder(Resource resource, String headerLine) {
		FlatFileItemWriterBuilder<Map<String, Object>> builder = new FlatFileItemWriterBuilder<>();
		builder.name("file-writer");
		builder.append(append);
		builder.encoding(file.getEncoding());
		builder.forceSync(forceSync);
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

	static class MapFieldExtractor implements FieldExtractor<Map<String, Object>> {

		private String[] names;

		public MapFieldExtractor(String[] names) {
			this.names = names;
		}

		@Override
		public Object[] extract(Map<String, Object> item) {
			Object[] fields = new Object[names.length];
			for (int index = 0; index < names.length; index++) {
				fields[index] = item.get(names[index]);
			}
			return fields;
		}

	}

	public AbstractFileItemWriter<Map<String, Object>> writer() {
		FileSystemResource resource = new FileSystemResource(path);
		switch (file.type(resource)) {
		case json:
			return jsonWriter(resource);
		case fixed:
			return formattedWriter(resource);
		default:
			return delimitedWriter(resource);
		}
	}

	private FlatFileItemWriter<Map<String, Object>> delimitedWriter(Resource resource) {
		FlatFileItemWriterBuilder<Map<String, Object>> builder = flatFileWriterBuilder(resource,
				file.isHeader() ? String.join(file.getDelimiter(), file.getNames()) : null);
		DelimitedBuilder<Map<String, Object>> delimited = builder.delimited();
		delimited.delimiter(file.getDelimiter());
		delimited.fieldExtractor(new MapFieldExtractor(file.getNames()));
		if (file.getNames().length > 0) {
			delimited.names(file.getNames());
		}
		return builder.build();
	}

	private FlatFileItemWriter<Map<String, Object>> formattedWriter(Resource resource) {
		FlatFileItemWriterBuilder<Map<String, Object>> builder = flatFileWriterBuilder(resource,
				file.isHeader() ? String.format(locale, format, Arrays.asList(file.getNames()).toArray()) : null);
		FormattedBuilder<Map<String, Object>> formatted = builder.formatted();
		formatted.fieldExtractor(new MapFieldExtractor(file.getNames()));
		if (file.getNames().length > 0) {
			formatted.names(file.getNames());
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

	@Override
	protected String targetDescription() {
		return String.format("file %s", path);
	}

}
