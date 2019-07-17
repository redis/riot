package com.redislabs.riot.cli;

import java.io.IOException;
import java.io.Writer;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileHeaderCallback;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder.DelimitedBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder.FormattedBuilder;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.FieldExtractor;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.batch.item.json.builder.JsonFileItemWriterBuilder;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;

@Command(name = "file", description = "File")
public class FileWriterCommand extends AbstractCommand {

	private final static Logger log = LoggerFactory.getLogger(FileWriterCommand.class);

	@ParentCommand
	private AbstractReaderCommand parent;

	@Mixin
	private FileOptions output = new FileOptions();
	@Option(names = "--append", description = "Append to file if it exists")
	boolean append;
	@Option(names = "--encoding", description = "File encoding", paramLabel = "<charset>")
	String encoding = FlatFileItemWriter.DEFAULT_CHARSET;
	@Option(names = "--force-sync", description = "Force-sync changes to disk on flush")
	boolean forceSync;
	@Option(names = "--header", description = "Write field names on first line")
	private boolean header = false;
	@Option(names = "--names", arity = "1..*", description = "Field names in the order they should appear in the file", paramLabel = "<string string>")
	private String[] names;
	@Option(names = "--line-separator", description = "String used to separate lines in output", paramLabel = "<string>")
	private String lineSeparator = FlatFileItemWriter.DEFAULT_LINE_SEPARATOR;
	@Option(names = "--delimiter", description = "Delimiter used when writing output", paramLabel = "<string>")
	private String delimiter = DelimitedLineTokenizer.DELIMITER_COMMA;
	@Option(names = "--format", description = "Format string used to aggregate items")
	private String format;
	@Option(names = "--locale", description = "Locale")
	private Locale locale = Locale.ENGLISH;
	@Option(names = "--max-length", description = "Max length of the formatted string")
	private Integer maxLength;
	@Option(names = "--min-length", description = "Min length of the formatted string")
	private Integer minLength;

	private ItemWriter<Map<String, Object>> jsonWriter() throws IOException {
		JsonFileItemWriterBuilder<Map<String, Object>> builder = new JsonFileItemWriterBuilder<>();
		builder.name("json-file-writer");
		builder.append(append);
		builder.encoding(encoding);
		builder.forceSync(forceSync);
		builder.jsonObjectMarshaller(new JacksonJsonObjectMarshaller<>());
		builder.lineSeparator(lineSeparator);
		builder.resource(output.resource());
		builder.saveState(false);
		return builder.build();
	}

	private FlatFileItemWriterBuilder<Map<String, Object>> flatFileWriterBuilder(String headerLine) throws IOException {
		FlatFileItemWriterBuilder<Map<String, Object>> builder = new FlatFileItemWriterBuilder<>();
		builder.name("file-writer");
		builder.append(append);
		builder.encoding(encoding);
		builder.forceSync(forceSync);
		builder.lineSeparator(lineSeparator);
		builder.resource(output.resource());
		builder.saveState(false);
		if (header) {
			builder.headerCallback(new FlatFileHeaderCallback() {

				@Override
				public void writeHeader(Writer writer) throws IOException {
					writer.write(headerLine);
				}
			});
		}
		return builder;
	}

	class MapFieldExtractor implements FieldExtractor<Map<String, Object>> {

		@Override
		public Object[] extract(Map<String, Object> item) {
			Object[] fields = new Object[names.length];
			for (int index = 0; index < names.length; index++) {
				fields[index] = item.get(names[index]);
			}
			return fields;
		}

	}

	private ItemWriter<Map<String, Object>> writer() throws IOException {
		switch (output.fileType()) {
		case json:
			return jsonWriter();
		case fixed:
			return fixedLengthWriter();
		default:
			return delimitedWriter();
		}
	}

	private ItemWriter<Map<String, Object>> delimitedWriter() throws IOException {
		FlatFileItemWriterBuilder<Map<String, Object>> builder = flatFileWriterBuilder(String.join(delimiter, names));
		DelimitedBuilder<Map<String, Object>> delimited = builder.delimited();
		delimited.delimiter(delimiter);
		delimited.fieldExtractor(new MapFieldExtractor());
		if (names != null) {
			delimited.names(names);
		}
		return builder.build();
	}

	private ItemWriter<Map<String, Object>> fixedLengthWriter() throws IOException {
		FlatFileItemWriterBuilder<Map<String, Object>> builder = flatFileWriterBuilder(
				String.format(locale, format, Arrays.asList(names).toArray()));
		FormattedBuilder<Map<String, Object>> formatted = builder.formatted();
		formatted.fieldExtractor(new MapFieldExtractor());
		if (names != null) {
			formatted.names(names);
		}
		formatted.format(format);
		formatted.locale(locale);
		if (maxLength != null) {
			formatted.maximumLength(maxLength);
		}
		if (minLength != null) {
			formatted.minimumLength(minLength);
		}
		return builder.build();
	}

	@Override
	public void run() {
		try {
			parent.execute(writer(), "file " + output.description());
		} catch (Exception e) {
			log.debug("Could not create file writer", e);
		}
	}

}
