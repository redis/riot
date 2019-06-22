package com.redislabs.riot.cli.file;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;

import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder.FormattedBuilder;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "ff", description = "Formatted file")
public class FormattedFileWriterCommand extends AbstractFlatFileWriterCommand {

	@Option(names = "--format", required = true, description = "Format string used to aggregate items.")
	private String format;
	@Option(names = "--locale", description = "Locale.")
	private Locale locale = Locale.ENGLISH;
	@Option(names = "--max-length", description = "Maximum length of the formatted string.")
	private Integer maxLength;
	@Option(names = "--min-length", description = "Minimum length of the formatted string.")
	private Integer minLength;

	@Override
	protected void configure(FlatFileItemWriterBuilder<Map<String, Object>> builder) {
		builder.name("formatted-file-writer");
		FormattedBuilder<Map<String, Object>> formatted = builder.formatted();
		formatted.fieldExtractor(fieldExtractor());
		formatted.format(format);
		formatted.locale(locale);
		if (maxLength != null) {
			formatted.maximumLength(maxLength);
		}
		if (minLength != null) {
			formatted.minimumLength(minLength);
		}
	}

	@Override
	protected String header(String[] names) {
		return String.format(locale, format, Arrays.asList(names).toArray());
	}

}
