package com.redislabs.riot.file;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.file.DefaultBufferedReaderFactory;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.separator.DefaultRecordSeparatorPolicy;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.Range;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;
import org.springframework.batch.item.xml.XmlItemReader;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;

import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

@Slf4j
@Command(name = "import", description = "Import file(s)")
public class MapFileImportCommand extends AbstractFileImportCommand<Map<String, Object>, Map<String, Object>> {

	@Mixin
	private MapFileOptions mapFileOptions = new MapFileOptions();
	@CommandLine.Option(names = "--skip", description = "Delimited/FW lines to skip at start (default: ${DEFAULT-VALUE})", paramLabel = "<count>")
	private int linesToSkip = 0;
	@CommandLine.Option(names = "--include", arity = "1..*", description = "Delimited/FW field indices to include (0-based)", paramLabel = "<index>")
	private int[] includedFields = new int[0];
	@CommandLine.Option(names = "--ranges", arity = "1..*", description = "Fixed-width column ranges", paramLabel = "<int>")
	private Range[] columnRanges = new Range[0];
	@CommandLine.Option(names = "--quote", description = "Escape character for delimited files (default: ${DEFAULT-VALUE})", paramLabel = "<char>")
	private Character quoteCharacter = DelimitedLineTokenizer.DEFAULT_QUOTE_CHARACTER;

	@Override
	@SuppressWarnings({ "rawtypes", "unchecked" })
	protected AbstractItemStreamItemReader<Map<String, Object>> reader(String file, FileType fileType,
			Resource resource) throws IOException {
		switch (fileType) {
		case DELIMITED:
			FlatFileItemReaderBuilder<Map<String, Object>> delimitedReaderBuilder = flatFileReaderBuilder(resource);
			FlatFileItemReaderBuilder.DelimitedBuilder<Map<String, Object>> delimitedBuilder = delimitedReaderBuilder
					.delimited();
			delimitedBuilder.delimiter(mapFileOptions.delimiter(file));
			delimitedBuilder.includedFields(includedFields());
			delimitedBuilder.quoteCharacter(quoteCharacter);
			String[] fieldNames = mapFileOptions.getNames();
			if (mapFileOptions.isHeader()) {
				BufferedReader reader = new DefaultBufferedReaderFactory().create(resource, fileOptions.getEncoding());
				DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
				tokenizer.setDelimiter(mapFileOptions.delimiter(file));
				tokenizer.setQuoteCharacter(quoteCharacter);
				if (includedFields.length > 0) {
					tokenizer.setIncludedFields(includedFields);
				}
				fieldNames = tokenizer.tokenize(reader.readLine()).getValues();
				log.debug("Found header {}", Arrays.asList(fieldNames));
			}
			if (fieldNames == null || fieldNames.length == 0) {
				throw new IOException("No fields specified");
			}
			delimitedBuilder.names(fieldNames);
			return delimitedReaderBuilder.build();
		case FIXED:
			FlatFileItemReaderBuilder<Map<String, Object>> fixedReaderBuilder = flatFileReaderBuilder(resource);
			FlatFileItemReaderBuilder.FixedLengthBuilder<Map<String, Object>> fixedLength = fixedReaderBuilder
					.fixedLength();
			Assert.notEmpty(columnRanges, "Column ranges are required");
			fixedLength.columns(columnRanges);
			fixedLength.names(mapFileOptions.getNames());
			return fixedReaderBuilder.build();
		case JSON:
			return (JsonItemReader) jsonReader(resource, Map.class);
		case XML:
			return (XmlItemReader) xmlReader(resource, Map.class);
		}
		throw new IllegalArgumentException("Unsuppored file type: " + fileType);
	}

	private Integer[] includedFields() {
		Integer[] fields = new Integer[includedFields.length];
		for (int index = 0; index < includedFields.length; index++) {
			fields[index] = includedFields[index];
		}
		return fields;
	}

	private FlatFileItemReaderBuilder<Map<String, Object>> flatFileReaderBuilder(Resource resource) {
		FlatFileItemReaderBuilder<Map<String, Object>> flatFileReaderBuilder = new FlatFileItemReaderBuilder<>();
		flatFileReaderBuilder.name("flat-file-reader");
		flatFileReaderBuilder.resource(resource);
		flatFileReaderBuilder.encoding(fileOptions.getEncoding());
		flatFileReaderBuilder.linesToSkip(linesToSkip);
		flatFileReaderBuilder.strict(true);
		flatFileReaderBuilder.saveState(false);
		flatFileReaderBuilder.fieldSetMapper(new MapFieldSetMapper());
		flatFileReaderBuilder.recordSeparatorPolicy(new DefaultRecordSeparatorPolicy());
		if (mapFileOptions.isHeader() && linesToSkip == 0) {
			flatFileReaderBuilder.linesToSkip(1);
		}
		return flatFileReaderBuilder;
	}

	@Override
	protected ItemProcessor<Map<String, Object>, Map<String, Object>> processor() {
		return mapProcessor();
	}

}
