package com.redislabs.riot.cli.file;

import java.io.IOException;
import java.io.Writer;
import java.net.MalformedURLException;
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
import org.springframework.core.io.Resource;

import com.redislabs.riot.cli.ExportCommand;
import com.redislabs.riot.cli.redis.RedisConnectionOptions;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;

@Command(name = "export", description = "Export to file")
public class FileExportCommand extends ExportCommand {

	@ParentCommand
	private FileConnector connector;
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

	private JsonFileItemWriter<Map<String, Object>> jsonWriter(Resource resource) {
		JsonFileItemWriterBuilder<Map<String, Object>> builder = new JsonFileItemWriterBuilder<>();
		builder.name("json-file-writer");
		builder.append(append);
		builder.encoding(connector.getEncoding());
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
		builder.encoding(connector.getEncoding());
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

	public AbstractFileItemWriter<Map<String, Object>> writer() throws MalformedURLException {
		switch (connector.type()) {
		case json:
			return jsonWriter(connector.resource());
		case fixed:
			return formattedWriter(connector.resource());
		default:
			return delimitedWriter(connector.resource());
		}
	}

	private FlatFileItemWriter<Map<String, Object>> delimitedWriter(Resource resource) {
		FlatFileItemWriterBuilder<Map<String, Object>> builder = flatFileWriterBuilder(resource,
				connector.isHeader() ? String.join(connector.getDelimiter(), connector.getNames()) : null);
		DelimitedBuilder<Map<String, Object>> delimited = builder.delimited();
		delimited.delimiter(connector.getDelimiter());
		delimited.fieldExtractor(new MapFieldExtractor(connector.getNames()));
		if (connector.getNames().length > 0) {
			delimited.names(connector.getNames());
		}
		return builder.build();
	}

	private FlatFileItemWriter<Map<String, Object>> formattedWriter(Resource resource) {
		FlatFileItemWriterBuilder<Map<String, Object>> builder = flatFileWriterBuilder(resource,
				connector.isHeader() ? String.format(locale, format, Arrays.asList(connector.getNames()).toArray())
						: null);
		FormattedBuilder<Map<String, Object>> formatted = builder.formatted();
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

	@Override
	protected String name() {
		return "file-export";
	}

	@Override
	protected RedisConnectionOptions redis() {
		return connector.riot().redis();
	}

}
