package com.redis.riot.file;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.springframework.batch.item.ItemReader;
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
import org.springframework.core.io.ProtocolResolver;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.google.cloud.spring.core.GcpScope;
import com.redis.riot.file.xml.XmlItemReader;
import com.redis.riot.file.xml.XmlItemReaderBuilder;
import com.redis.riot.file.xml.XmlObjectReader;

public class FileReaderRegistry extends AbstractFactoryRegistry<ItemReader<?>, ReadOptions> {

	private ProtocolResolver stdInProtocolResolver = new StdInProtocolResolver();

	public ProtocolResolver getStdInProtocolResolver() {
		return stdInProtocolResolver;
	}

	public void setStdInProtocolResolver(ProtocolResolver stdInProtocolResolver) {
		this.stdInProtocolResolver = stdInProtocolResolver;
	}

	@Override
	protected Collection<ProtocolResolver> protocolResolvers(ReadOptions options) {
		List<ProtocolResolver> resolvers = new ArrayList<>(super.protocolResolvers(options));
		resolvers.add(stdInProtocolResolver);
		return resolvers;
	}

	@Override
	protected GcpScope googleStorageScope() {
		return GcpScope.STORAGE_READ_ONLY;
	}

	@Override
	protected Resource gzip(Resource resource) throws IOException {
		InputStream inputStream = resource.getInputStream();
		GZIPInputStream gzipInputStream = new GZIPInputStream(inputStream);
		return new NamedInputStreamResource(gzipInputStream, resource.getFilename(), resource.getDescription());
	}

	public FlatFileItemReader<Map<String, Object>> delimited(Resource resource, ReadOptions options) {
		return flatFileReader(resource, delimitedLineTokenizer(delimiter(resource, options), options), options);
	}

	public FlatFileItemReader<Map<String, Object>> fixedWidth(Resource resource, ReadOptions options) {
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

	public JsonItemReader<Object> json(Resource resource, ReadOptions options) {
		JsonItemReaderBuilder<Object> builder = new JsonItemReaderBuilder<>();
		builder.name(resource.getFilename() + "-json-file-reader");
		builder.resource(resource);
		builder.saveState(false);
		JacksonJsonObjectReader<Object> objectReader = new JacksonJsonObjectReader<>(options.getItemType());
		objectReader.setMapper(objectMapper(new ObjectMapper(), options));
		builder.jsonObjectReader(objectReader);
		if (options.getMaxItemCount() > 0) {
			builder.maxItemCount(options.getMaxItemCount());
		}
		return builder.build();
	}

	public FlatFileItemReader<?> jsonLines(Resource resource, ReadOptions options) {
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

	public XmlItemReader<Object> xml(Resource resource, ReadOptions options) {
		XmlItemReaderBuilder<Object> builder = new XmlItemReaderBuilder<>();
		builder.name(resource.getFilename() + "-xml-file-reader");
		builder.resource(resource);
		XmlObjectReader<Object> objectReader = new XmlObjectReader<>(options.getItemType());
		objectReader.setMapper(objectMapper(new XmlMapper(), options));
		builder.xmlObjectReader(objectReader);
		if (options.getMaxItemCount() > 0) {
			builder.maxItemCount(options.getMaxItemCount());
		}
		return builder.build();
	}

	private DelimitedLineTokenizer delimitedLineTokenizer(String delimiter, ReadOptions options) {
		DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
		tokenizer.setDelimiter(delimiter);
		tokenizer.setQuoteCharacter(options.getQuoteCharacter());
		if (!ObjectUtils.isEmpty(options.getIncludedFields())) {
			tokenizer.setIncludedFields(includedFields(options));
		}
		return tokenizer;
	}

	private int[] includedFields(ReadOptions options) {
		return options.getIncludedFields().stream().mapToInt(Integer::intValue).toArray();
	}

	private FlatFileItemReader<Map<String, Object>> flatFileReader(Resource resource, AbstractLineTokenizer tokenizer,
			ReadOptions options) {
		if (ObjectUtils.isEmpty(options.getFields())) {
			Assert.isTrue(options.isHeader(),
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

	private <T> FlatFileItemReaderBuilder<T> flatFileReader(Resource resource, ReadOptions options) {
		FlatFileItemReaderBuilder<T> builder = new FlatFileItemReaderBuilder<>();
		builder.resource(resource);
		if (options.getMaxItemCount() > 0) {
			builder.maxItemCount(options.getMaxItemCount());
		}
		builder.encoding(options.getEncoding());
		builder.recordSeparatorPolicy(recordSeparatorPolicy(options));
		builder.linesToSkip(linesToSkip(options));
		builder.saveState(false);
		return builder;
	}

	private RecordSeparatorPolicy recordSeparatorPolicy(ReadOptions options) {
		String quoteCharacter = String.valueOf(options.getQuoteCharacter());
		return new DefaultRecordSeparatorPolicy(quoteCharacter, options.getContinuationString());
	}

	private int headerIndex(ReadOptions options) {
		if (options.getHeaderLine() != null) {
			return options.getHeaderLine();
		}
		return linesToSkip(options) - 1;
	}

	private int linesToSkip(ReadOptions options) {
		if (options.getLinesToSkip() != null) {
			return options.getLinesToSkip();
		}
		if (options.isHeader()) {
			return 1;
		}
		return 0;
	}

	private <T extends ObjectMapper> T objectMapper(T objectMapper, ReadOptions options) {
		objectMapper.configure(DeserializationFeature.USE_LONG_FOR_INTS, true);
		SimpleModule module = new SimpleModule();
		options.getDeserializers().forEach(module::addDeserializer);
		objectMapper.registerModule(module);
		return objectMapper;
	}

	public static FileReaderRegistry defaultReaderRegistry() {
		FileReaderRegistry registry = new FileReaderRegistry();
		registry.register(FileUtils.JSON, registry::json);
		registry.register(FileUtils.JSON_LINES, registry::jsonLines);
		registry.register(FileUtils.XML, registry::xml);
		registry.register(FileUtils.CSV, registry::delimited);
		registry.register(FileUtils.PSV, registry::delimited);
		registry.register(FileUtils.TSV, registry::delimited);
		registry.register(FileUtils.TEXT, registry::fixedWidth);
		return registry;
	}

}
