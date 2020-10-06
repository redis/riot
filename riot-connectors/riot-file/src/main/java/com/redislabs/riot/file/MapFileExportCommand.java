package com.redislabs.riot.file;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.transform.FieldExtractor;
import org.springframework.batch.item.redis.support.KeyValue;
import org.springframework.batch.item.resource.FlatResourceItemWriterBuilder;
import org.springframework.core.io.Resource;
import org.springframework.core.io.WritableResource;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

@Command(name = "export", description = "Export to a file")
public class MapFileExportCommand extends AbstractFileExportCommand<Map<String, Object>> {

	@Mixin
	private MapFileOptions mapFileOptions = new MapFileOptions();
	@CommandLine.Option(names = "--line-format", description = "Format for line aggregation", paramLabel = "<string>")
	private String lineFormat;
	@CommandLine.Option(names = "--locale", description = "Locale", paramLabel = "<tag>")
	private Locale locale = Locale.ENGLISH;
	@CommandLine.Option(names = "--max-length", description = "Max length of the formatted string", paramLabel = "<int>")
	private Integer maxLength;
	@CommandLine.Option(names = "--min-length", description = "Min length of the formatted string", paramLabel = "<int>")
	private Integer minLength;

	@Override
	protected ItemProcessor<KeyValue<String>, Map<String, Object>> processor() {
		return keyValueMapItemProcessor();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected ItemWriter<Map<String, Object>> writer(FileType fileType, WritableResource resource) throws IOException {
		switch (fileType) {
		case DELIMITED:
			FlatResourceItemWriterBuilder<Map<String, String>> delimitedWriterBuilder = flatWriterBuilder(
					"delimited-resource-item-writer", resource);
			if (mapFileOptions.isHeader()) {
				delimitedWriterBuilder.headerCallback(
						w -> w.write(String.join(mapFileOptions.delimiter(file), mapFileOptions.getNames())));
			}
			FlatResourceItemWriterBuilder.DelimitedBuilder<Map<String, String>> delimited = delimitedWriterBuilder
					.delimited();
			delimited.delimiter(mapFileOptions.delimiter(file));
			delimited.fieldExtractor(new MapFieldExtractor());
			if (mapFileOptions.getNames().length > 0) {
				delimited.names(mapFileOptions.getNames());
			}
			return (ItemWriter) delimitedWriterBuilder.build();
		case FIXED:
			FlatResourceItemWriterBuilder<Map<String, String>> fixedWriterBuilder = flatWriterBuilder(
					"formatted-resource-item-writer", resource);
			if (mapFileOptions.isHeader()) {
				fixedWriterBuilder.headerCallback(
						w -> w.write(String.format(locale, lineFormat, (Object[]) mapFileOptions.getNames())));
			}
			FlatResourceItemWriterBuilder.FormattedBuilder<Map<String, String>> formatted = fixedWriterBuilder
					.formatted();
			formatted.fieldExtractor(new MapFieldExtractor());
			if (mapFileOptions.getNames().length > 0) {
				formatted.names(mapFileOptions.getNames());
			}
			formatted.format(lineFormat);
			formatted.locale(locale);
			if (minLength != null) {
				formatted.minimumLength(minLength);
			}
			if (maxLength != null) {
				formatted.maximumLength(maxLength);
			}
			return (ItemWriter) fixedWriterBuilder.build();
		case JSON:
			return jsonWriter(resource);
		case XML:
			return xmlWriter(resource);
		}
		throw new IllegalArgumentException("Unsupported file type: " + fileType);
	}

	private class MapFieldExtractor implements FieldExtractor<Map<String, String>> {

		@Override
		public String[] extract(Map<String, String> item) {
			String[] names = mapFileOptions.getNames();
			String[] fields = new String[names.length];
			for (int index = 0; index < names.length; index++) {
				fields[index] = item.get(names[index]);
			}
			return fields;
		}

	}

	private FlatResourceItemWriterBuilder<Map<String, String>> flatWriterBuilder(String name, Resource resource) {
		FlatResourceItemWriterBuilder<Map<String, String>> builder = new FlatResourceItemWriterBuilder<>();
		builder.name(name);
		builder.append(append);
		builder.encoding(fileOptions.getEncoding());
		builder.lineSeparator(lineSeparator);
		builder.resource(resource);
		builder.saveState(false);
		return builder;
	}
}
