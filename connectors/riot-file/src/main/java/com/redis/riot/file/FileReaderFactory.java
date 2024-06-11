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
	private Class<?> itemType = Map.class;

	public ItemReader<?> create(Resource resource) throws Exception {
		FileType type = args.fileType(resource);
		switch (type) {
		case CSV:
			return flatFileReader(resource, delimitedLineTokenizer(resource));
		case FIXED:
			return flatFileReader(resource, fixedLengthTokenizer());
		case XML:
			return xmlReader(resource);
		case JSON:
			return jsonReader(resource);
		case JSONL:
			return jsonlReader(resource);
		default:
			throw new UnsupportedOperationException("Unsupported file type: " + type);
		}
	}

	private DelimitedLineTokenizer delimitedLineTokenizer(Resource resource) throws Exception {
		DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
		tokenizer.setDelimiter(delimiter(resource));
		tokenizer.setQuoteCharacter(args.getQuoteCharacter());
		if (!ObjectUtils.isEmpty(args.getIncludedFields())) {
			tokenizer.setIncludedFields(args.getIncludedFields().stream().mapToInt(Integer::intValue).toArray());
		}
		tokenizer.afterPropertiesSet();
		return tokenizer;
	}

	private FlatFileItemReader<Map<String, Object>> flatFileReader(Resource resource, AbstractLineTokenizer tokenizer) {
		if (ObjectUtils.isEmpty(args.getFields())) {
			Assert.isTrue(args.isHeader(), "No field names specified and header not enabled");
		} else {
			tokenizer.setNames(args.getFields().toArray(new String[0]));
		}
		FlatFileItemReaderBuilder<Map<String, Object>> builder = flatFileReader(resource);
		builder.lineTokenizer(tokenizer);
		builder.skippedLinesCallback(new HeaderCallbackHandler(tokenizer, headerIndex()));
		return builder.build();
	}

	private FlatFileItemReaderBuilder<Map<String, Object>> flatFileReader(Resource resource) {
		FlatFileItemReaderBuilder<Map<String, Object>> builder = new FlatFileItemReaderBuilder<>();
		builder.resource(resource);
		builder.maxItemCount(args.getMaxItemCount());
		if (args.getEncoding() != null) {
			builder.encoding(args.getEncoding());
		}
		builder.recordSeparatorPolicy(recordSeparatorPolicy());
		builder.linesToSkip(linesToSkip());
		builder.strict(true);
		builder.saveState(false);
		builder.fieldSetMapper(new MapFieldSetMapper());
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

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private FlatFileItemReader jsonlReader(Resource resource) {
		FlatFileItemReaderBuilder reader = flatFileReader(resource);
		reader.lineMapper(jsonLineMapper());
		return reader.build();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private XmlItemReader xmlReader(Resource resource) {
		XmlItemReaderBuilder builder = new XmlItemReaderBuilder<>();
		builder.name(resource.getFilename() + "-xml-file-reader");
		builder.resource(resource);
		XmlObjectReader objectReader = new XmlObjectReader<>(itemType);
		objectReader.setMapper(objectMapper(new XmlMapper()));
		builder.xmlObjectReader(objectReader);
		builder.maxItemCount(args.getMaxItemCount());
		return builder.build();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private ItemReader jsonReader(Resource resource) {
		JsonItemReaderBuilder builder = new JsonItemReaderBuilder<>();
		builder.name(resource.getFilename() + "-json-file-reader");
		builder.resource(resource);
		JacksonJsonObjectReader objectReader = new JacksonJsonObjectReader<>(itemType);
		objectReader.setMapper(objectMapper(new ObjectMapper()));
		builder.jsonObjectReader(objectReader);
		builder.maxItemCount(args.getMaxItemCount());
		return builder.build();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private LineMapper jsonLineMapper() {
		if (isTypeMap()) {
			return new JsonLineMapper();
		}
		return new ObjectMapperLineMapper(objectMapper(new ObjectMapper()), itemType);
	}

	private boolean isTypeMap() {
		return Map.class.isAssignableFrom(itemType);
	}

	private String delimiter(Resource resource) {
		if (args.getDelimiter() != null) {
			return args.getDelimiter();
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
		return new DefaultRecordSeparatorPolicy(String.valueOf(args.getQuoteCharacter()), args.getContinuationString());
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
		if (args.isHeader()) {
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

	public Class<?> getItemType() {
		return itemType;
	}

	public void setItemType(Class<?> itemType) {
		this.itemType = itemType;
	}

	public FileReaderArgs getArgs() {
		return args;
	}

	public void setArgs(FileReaderArgs options) {
		this.args = options;
	}

}
