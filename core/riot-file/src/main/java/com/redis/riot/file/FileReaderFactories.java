package com.redis.riot.file;

import java.util.List;
import java.util.Map;

import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.JsonLineMapper;
import org.springframework.batch.item.file.separator.DefaultRecordSeparatorPolicy;
import org.springframework.batch.item.file.separator.RecordSeparatorPolicy;
import org.springframework.batch.item.file.transform.AbstractLineTokenizer;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.FixedLengthTokenizer;
import org.springframework.batch.item.file.transform.Range;
import org.springframework.batch.item.file.transform.RangeArrayPropertyEditor;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.json.builder.JsonItemReaderBuilder;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redis.riot.file.xml.XmlItemReader;
import com.redis.riot.file.xml.XmlItemReaderBuilder;
import com.redis.riot.file.xml.XmlObjectReader;

public class FileReaderFactories {

	public static FlatFileItemReader<Map<String, Object>> delimited(Resource resource, FileReaderOptions options) {
		String delimiter = options.getFileOptions().getDelimiter().orElseGet(() -> Files.delimiter(resource));
		return flatFileReader(resource, delimitedLineTokenizer(delimiter, options), options);
	}

	public static FlatFileItemReader<Map<String, Object>> fixedWidth(Resource resource, FileReaderOptions options) {
		FixedLengthTokenizer tokenizer = new FixedLengthTokenizer();
		RangeArrayPropertyEditor editor = new RangeArrayPropertyEditor();
		List<String> columnRanges = options.getColumnRanges();
		Assert.notEmpty(columnRanges, "Column ranges are required");
		editor.setAsText(String.join(",", columnRanges));
		Range[] ranges = (Range[]) editor.getValue();
		Assert.notEmpty(ranges, "Invalid ranges specified: " + columnRanges);
		tokenizer.setColumns(ranges);
		return flatFileReader(resource, tokenizer, options);
	}

	public static JsonItemReader<Object> json(Resource resource, FileReaderOptions options) {
		JsonItemReaderBuilder<Object> builder = new JsonItemReaderBuilder<>();
		builder.name(resource.getFilename() + "-json-file-reader");
		builder.resource(resource);
		builder.saveState(false);
		JacksonJsonObjectReader<Object> objectReader = new JacksonJsonObjectReader<>(options.getItemType());
		objectReader.setMapper(objectMapper(new ObjectMapper(), options));
		builder.jsonObjectReader(objectReader);
		options.getMaxItemCount().ifPresent(builder::maxItemCount);
		return builder.build();
	}

	public static FlatFileItemReader<?> jsonLines(Resource resource, FileReaderOptions options) {
		if (Map.class.isAssignableFrom(options.getItemType())) {
			FlatFileItemReaderBuilder<Map<String, Object>> reader = flatFileReader(resource, options);
			reader.lineMapper(new JsonLineMapper());
			reader.fieldSetMapper(new MapFieldSetMapper());
			return reader.build();
		}
		FlatFileItemReaderBuilder<Object> reader = flatFileReader(resource, options);
		ObjectMapper objectMapper = objectMapper(new ObjectMapper(), options);
		reader.lineMapper(new ObjectMapperLineMapper<>(objectMapper, options.getItemType()));
		return reader.build();
	}

	public static XmlItemReader<Object> xml(Resource resource, FileReaderOptions options) {
		XmlItemReaderBuilder<Object> builder = new XmlItemReaderBuilder<>();
		builder.name(resource.getFilename() + "-xml-file-reader");
		builder.resource(resource);
		XmlObjectReader<Object> objectReader = new XmlObjectReader<>(options.getItemType());
		objectReader.setMapper(objectMapper(new XmlMapper(), options));
		builder.xmlObjectReader(objectReader);
		options.getMaxItemCount().ifPresent(builder::maxItemCount);
		return builder.build();
	}

	private static DelimitedLineTokenizer delimitedLineTokenizer(String delimiter, FileReaderOptions options) {
		DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
		tokenizer.setDelimiter(delimiter);
		tokenizer.setQuoteCharacter(options.getFileOptions().getQuoteCharacter());
		if (!ObjectUtils.isEmpty(options.getIncludedFields())) {
			tokenizer.setIncludedFields(includedFields(options));
		}
		return tokenizer;
	}

	private static int[] includedFields(FileReaderOptions options) {
		return options.getIncludedFields().stream().mapToInt(Integer::intValue).toArray();
	}

	private static FlatFileItemReader<Map<String, Object>> flatFileReader(Resource resource,
			AbstractLineTokenizer tokenizer, FileReaderOptions options) {
		if (ObjectUtils.isEmpty(options.getFields())) {
			Assert.isTrue(options.getFileOptions().isHeader(),
					String.format("Could not create reader for file '%s': no header or field names specified",
							resource.getFilename()));
		} else {
			tokenizer.setNames(options.getFields().toArray(new String[0]));
		}
		FlatFileItemReaderBuilder<Map<String, Object>> builder = flatFileReader(resource, options);
		builder.fieldSetMapper(new MapFieldSetMapper());
		builder.lineTokenizer(tokenizer);
		builder.skippedLinesCallback(new HeaderCallbackHandler(tokenizer, headerIndex(options)));
		return builder.build();
	}

	private static <T> FlatFileItemReaderBuilder<T> flatFileReader(Resource resource, FileReaderOptions options) {
		FlatFileItemReaderBuilder<T> builder = new FlatFileItemReaderBuilder<>();
		builder.resource(resource);
		options.getMaxItemCount().ifPresent(builder::maxItemCount);
		builder.encoding(options.getFileOptions().getEncoding());
		builder.recordSeparatorPolicy(recordSeparatorPolicy(options));
		builder.linesToSkip(linesToSkip(options));
		builder.saveState(false);
		return builder;
	}

	private static RecordSeparatorPolicy recordSeparatorPolicy(FileReaderOptions options) {
		String quoteCharacter = String.valueOf(options.getFileOptions().getQuoteCharacter());
		return new DefaultRecordSeparatorPolicy(quoteCharacter, options.getContinuationString());
	}

	private static int headerIndex(FileReaderOptions options) {
		if (options.getHeaderLine() != null) {
			return options.getHeaderLine();
		}
		return linesToSkip(options) - 1;
	}

	private static int linesToSkip(FileReaderOptions options) {
		if (options.getLinesToSkip() != null) {
			return options.getLinesToSkip();
		}
		if (options.getFileOptions().isHeader()) {
			return 1;
		}
		return 0;
	}

	private static <T extends ObjectMapper> T objectMapper(T objectMapper, FileReaderOptions options) {
		objectMapper.configure(DeserializationFeature.USE_LONG_FOR_INTS, true);
		SimpleModule module = new SimpleModule();
		options.getDeserializers().forEach(module::addDeserializer);
		objectMapper.registerModule(module);
		return objectMapper;
	}

}
