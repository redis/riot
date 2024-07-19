package com.redis.riot.file;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
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
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redis.riot.file.xml.XmlItemReader;
import com.redis.riot.file.xml.XmlItemReaderBuilder;
import com.redis.riot.file.xml.XmlObjectReader;

public class FileReaderFactory {

	public static final String PIPE_DELIMITER = "|";

	private FileReaderArgs args = new FileReaderArgs();
	private Map<Class<?>, JsonDeserializer<?>> deserializers = new HashMap<>();

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public <T> ItemReader<T> createReader(Resource resource, FileType fileType, Class<T> itemType) {
		switch (fileType) {
		case XML:
			return xmlReader(resource, itemType);
		case JSON:
			return jsonReader(resource, itemType);
		case JSONL:
			return jsonlReader(resource, itemType);
		case CSV:
			return (FlatFileItemReader) flatFileReader(resource, delimitedLineTokenizer(delimiter(resource)));
		case FIXED:
			return (FlatFileItemReader) flatFileReader(resource, fixedLengthTokenizer());
		default:
			throw new UnsupportedOperationException("Unsupported file type: " + fileType);
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private <T> FlatFileItemReader<T> jsonlReader(Resource resource, Class<T> itemType) {
		if (Map.class.isAssignableFrom(itemType)) {
			FlatFileItemReaderBuilder<Map<String, Object>> reader = flatFileReader(resource);
			return (FlatFileItemReader) reader.fieldSetMapper(new MapFieldSetMapper()).lineMapper(new JsonLineMapper())
					.build();
		}
		FlatFileItemReaderBuilder<T> reader = flatFileReader(resource);
		return reader.lineMapper(lineMapper(itemType)).build();
	}

	private <T> LineMapper<T> lineMapper(Class<T> itemType) {
		return new ObjectMapperLineMapper<>(objectMapper(new ObjectMapper()), itemType);
	}

	private DelimitedLineTokenizer delimitedLineTokenizer(String delimiter) {
		DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
		tokenizer.setDelimiter(delimiter);
		tokenizer.setQuoteCharacter(args.getFileArgs().getQuoteCharacter());
		if (!ObjectUtils.isEmpty(args.getIncludedFields())) {
			tokenizer.setIncludedFields(args.getIncludedFields().stream().mapToInt(Integer::intValue).toArray());
		}
		return tokenizer;
	}

	private FlatFileItemReader<Map<String, Object>> flatFileReader(Resource resource, AbstractLineTokenizer tokenizer) {
		if (ObjectUtils.isEmpty(args.getFields())) {
			Assert.isTrue(args.getFileArgs().isHeader(),
					String.format("Could not create reader for file '%s': no header or field names specified",
							resource.getFilename()));
		} else {
			tokenizer.setNames(args.getFields().toArray(new String[0]));
		}
		FlatFileItemReaderBuilder<Map<String, Object>> builder = flatFileReader(resource);
		builder.fieldSetMapper(new MapFieldSetMapper());
		builder.lineTokenizer(tokenizer);
		builder.skippedLinesCallback(new HeaderCallbackHandler(tokenizer, headerIndex()));
		return builder.build();
	}

	private <T> FlatFileItemReaderBuilder<T> flatFileReader(Resource resource) {
		FlatFileItemReaderBuilder<T> builder = new FlatFileItemReaderBuilder<>();
		builder.resource(resource);
		builder.maxItemCount(args.getMaxItemCount());
		if (args.getFileArgs().getEncoding() != null) {
			builder.encoding(args.getFileArgs().getEncoding());
		}
		builder.recordSeparatorPolicy(recordSeparatorPolicy());
		builder.linesToSkip(linesToSkip());
		builder.strict(true);
		builder.saveState(false);
		return builder;
	}

	private FixedLengthTokenizer fixedLengthTokenizer() {
		FixedLengthTokenizer tokenizer = new FixedLengthTokenizer();
		RangeArrayPropertyEditor editor = new RangeArrayPropertyEditor();
		Assert.notEmpty(args.getColumnRanges(), "Column ranges are required");
		editor.setAsText(String.join(",", args.getColumnRanges()));
		Range[] ranges = (Range[]) editor.getValue();
		Assert.notEmpty(ranges, "Invalid ranges specified: " + args.getColumnRanges());
		tokenizer.setColumns(ranges);
		return tokenizer;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private <T extends ObjectMapper> T objectMapper(T objectMapper) {
		objectMapper.configure(DeserializationFeature.USE_LONG_FOR_INTS, true);
		if (!CollectionUtils.isEmpty(deserializers)) {
			SimpleModule module = new SimpleModule();
			for (Entry<Class<?>, JsonDeserializer<?>> entry : deserializers.entrySet()) {
				module.addDeserializer((Class) entry.getKey(), entry.getValue());
			}
			objectMapper.registerModule(module);
		}
		return objectMapper;
	}

	private <T> XmlItemReader<T> xmlReader(Resource resource, Class<T> itemType) {
		XmlItemReaderBuilder<T> builder = new XmlItemReaderBuilder<>();
		builder.name(resource.getFilename() + "-xml-file-reader");
		builder.resource(resource);
		XmlObjectReader<T> objectReader = new XmlObjectReader<>(itemType);
		objectReader.setMapper(objectMapper(new XmlMapper()));
		builder.xmlObjectReader(objectReader);
		builder.maxItemCount(args.getMaxItemCount());
		return builder.build();
	}

	private <T> JsonItemReader<T> jsonReader(Resource resource, Class<T> itemType) {
		JsonItemReaderBuilder<T> builder = new JsonItemReaderBuilder<>();
		builder.name(resource.getFilename() + "-json-file-reader");
		builder.resource(resource);
		JacksonJsonObjectReader<T> objectReader = new JacksonJsonObjectReader<>(itemType);
		objectReader.setMapper(objectMapper(new ObjectMapper()));
		builder.jsonObjectReader(objectReader);
		builder.maxItemCount(args.getMaxItemCount());
		return builder.build();
	}

	private String delimiter(Resource resource) {
		if (args.getFileArgs().getDelimiter() != null) {
			return args.getFileArgs().getDelimiter();
		}
		String extension = FileUtils.fileExtension(resource);
		if (extension == null) {
			return DelimitedLineTokenizer.DELIMITER_COMMA;
		}
		switch (extension) {
		case FileUtils.PSV:
			return PIPE_DELIMITER;
		case FileUtils.TSV:
			return DelimitedLineTokenizer.DELIMITER_TAB;
		default:
			return DelimitedLineTokenizer.DELIMITER_COMMA;
		}
	}

	private RecordSeparatorPolicy recordSeparatorPolicy() {
		return new DefaultRecordSeparatorPolicy(String.valueOf(args.getFileArgs().getQuoteCharacter()),
				args.getContinuationString());
	}

	private int headerIndex() {
		if (args.getHeaderLine() != null) {
			return args.getHeaderLine();
		}
		return linesToSkip() - 1;
	}

	private int linesToSkip() {
		if (args.getLinesToSkip() != null) {
			return args.getLinesToSkip();
		}
		if (args.getFileArgs().isHeader()) {
			return 1;
		}
		return 0;
	}

	public Map<Class<?>, JsonDeserializer<?>> getDeserializers() {
		return deserializers;
	}

	public void setDeserializers(Map<Class<?>, JsonDeserializer<?>> deserializers) {
		this.deserializers = deserializers;
	}

	public <T> void addDeserializer(Class<T> type, JsonDeserializer<T> deserializer) {
		deserializers.put(type, deserializer);
	}

	public FileReaderArgs getArgs() {
		return args;
	}

	public void setArgs(FileReaderArgs options) {
		this.args = options;
	}

}
