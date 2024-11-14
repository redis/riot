package com.redis.riot.file;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.transform.LineAggregator;
import org.springframework.batch.item.file.transform.PassThroughFieldExtractor;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.core.io.ProtocolResolver;
import org.springframework.core.io.Resource;
import org.springframework.core.io.WritableResource;
import org.springframework.util.CollectionUtils;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.google.cloud.spring.core.GcpScope;
import com.redis.riot.file.xml.XmlResourceItemWriterBuilder;
import com.redis.riot.resource.FlatFileItemWriter;
import com.redis.riot.resource.FlatFileItemWriterBuilder;
import com.redis.riot.resource.FlatFileItemWriterBuilder.DelimitedBuilder;
import com.redis.riot.resource.FlatFileItemWriterBuilder.FormattedBuilder;
import com.redis.riot.resource.JsonFileItemWriterBuilder;

public class FileWriterRegistry extends AbstractFactoryRegistry<ItemWriter<?>, WriteOptions> {

	private ProtocolResolver stdOutProtocolResolver = new StdOutProtocolResolver();

	public ProtocolResolver getStdOutProtocolResolver() {
		return stdOutProtocolResolver;
	}

	public void setStdOutProtocolResolver(ProtocolResolver resolver) {
		this.stdOutProtocolResolver = resolver;
	}

	@Override
	protected GcpScope googleStorageScope() {
		return GcpScope.STORAGE_READ_WRITE;
	}

	@Override
	protected Collection<ProtocolResolver> protocolResolvers(WriteOptions options) {
		List<ProtocolResolver> resolvers = new ArrayList<>(super.protocolResolvers(options));
		resolvers.add(stdOutProtocolResolver);
		return resolvers;
	}

	@Override
	protected Resource gzip(Resource resource) throws IOException {
		OutputStream outputStream = ((WritableResource) resource).getOutputStream();
		GZIPOutputStream gzipOutputStream = new GZIPOutputStream(outputStream);
		return new OutputStreamResource(gzipOutputStream, resource.getFilename(), resource.getDescription());
	}

	private FlatFileItemWriter<Map<String, Object>> delimited(Resource resource, WriteOptions options) {
		FlatFileItemWriterBuilder<Map<String, Object>> writer = flatFileWriter(resource, options);
		DelimitedBuilder<Map<String, Object>> delimitedBuilder = writer.delimited();
		delimitedBuilder.delimiter(delimiter(resource, options));
		delimitedBuilder.fieldExtractor(new PassThroughFieldExtractor<>());
		delimitedBuilder.quoteCharacter(String.valueOf(options.getQuoteCharacter()));
		return flatFileWriter(writer, delimitedBuilder.build(), options);
	}

	private FlatFileItemWriter<Map<String, Object>> formatted(Resource resource, WriteOptions options) {
		FlatFileItemWriterBuilder<Map<String, Object>> writer = flatFileWriter(resource, options);
		FormattedBuilder<Map<String, Object>> formattedBuilder = writer.formatted();
		formattedBuilder.format(options.getFormatterString());
		formattedBuilder.fieldExtractor(new PassThroughFieldExtractor<>());
		return flatFileWriter(writer, formattedBuilder.build(), options);
	}

	private FlatFileItemWriter<?> jsonLines(Resource resource, WriteOptions options) {
		FlatFileItemWriterBuilder<?> builder = flatFileWriter(resource, options);
		builder.lineAggregator(new JsonLineAggregator<>(new ObjectMapper()));
		return builder.build();
	}

	private ItemWriter<?> json(Resource resource, WriteOptions options) {
		JsonFileItemWriterBuilder<?> writer = new JsonFileItemWriterBuilder<>();
		writer.name(resource.getFilename());
		writer.resource((WritableResource) resource);
		writer.append(options.isAppend());
		writer.encoding(options.getEncoding());
		writer.forceSync(options.isForceSync());
		writer.lineSeparator(options.getLineSeparator());
		writer.saveState(false);
		writer.shouldDeleteIfEmpty(options.isShouldDeleteIfEmpty());
		writer.shouldDeleteIfExists(options.isShouldDeleteIfExists());
		writer.transactional(options.isTransactional());
		writer.jsonObjectMarshaller(new JacksonJsonObjectMarshaller<>(objectMapper(new ObjectMapper())));
		return writer.build();
	}

	private <T extends ObjectMapper> T objectMapper(T objectMapper) {
		objectMapper.setSerializationInclusion(Include.NON_NULL);
		objectMapper.setSerializationInclusion(Include.NON_DEFAULT);
		return objectMapper;
	}

	private ItemWriter<?> xml(Resource resource, WriteOptions options) {
		XmlResourceItemWriterBuilder<?> writer = new XmlResourceItemWriterBuilder<>();
		writer.name(resource.getFilename());
		writer.append(options.isAppend());
		writer.encoding(options.getEncoding());
		writer.lineSeparator(options.getLineSeparator());
		writer.rootName(options.getRootName());
		writer.resource((WritableResource) resource);
		writer.saveState(false);
		XmlMapper mapper = objectMapper(new XmlMapper());
		mapper.setConfig(mapper.getSerializationConfig().withRootName(options.getElementName()));
		writer.xmlObjectMarshaller(new JacksonJsonObjectMarshaller<>(mapper));
		return writer.build();
	}

	private <T> FlatFileItemWriterBuilder<T> flatFileWriter(Resource resource, WriteOptions options) {
		FlatFileItemWriterBuilder<T> builder = new FlatFileItemWriterBuilder<>();
		builder.name(resource.getFilename());
		builder.resource((WritableResource) resource);
		builder.append(options.isAppend());
		builder.encoding(options.getEncoding());
		builder.forceSync(options.isForceSync());
		builder.lineSeparator(options.getLineSeparator());
		builder.saveState(false);
		builder.shouldDeleteIfEmpty(options.isShouldDeleteIfEmpty());
		builder.shouldDeleteIfExists(options.isShouldDeleteIfExists());
		builder.transactional(options.isTransactional());
		return builder;
	}

	private FlatFileItemWriter<Map<String, Object>> flatFileWriter(
			FlatFileItemWriterBuilder<Map<String, Object>> writer, LineAggregator<Map<String, Object>> aggregator,
			WriteOptions options) {
		writer.lineAggregator(aggregator);
		if (options.isHeader()) {
			Map<String, Object> headerRecord = options.getHeaderSupplier().get();
			if (!CollectionUtils.isEmpty(headerRecord)) {
				List<String> fields = new ArrayList<>(headerRecord.keySet());
				Collections.sort(fields);
				Map<String, Object> fieldMap = new LinkedHashMap<>();
				fields.forEach(f -> fieldMap.put(f, f));
				String headerLine = aggregator.aggregate(fieldMap);
				writer.headerCallback(w -> w.write(headerLine));
			}
		}
		return writer.build();
	}

	public static FileWriterRegistry defaultWriterRegistry() {
		FileWriterRegistry registry = new FileWriterRegistry();
		registry.register(FileUtils.JSON, registry::json);
		registry.register(FileUtils.JSON_LINES, registry::jsonLines);
		registry.register(FileUtils.XML, registry::xml);
		registry.register(FileUtils.CSV, registry::delimited);
		registry.register(FileUtils.PSV, registry::delimited);
		registry.register(FileUtils.TSV, registry::delimited);
		registry.register(FileUtils.TEXT, registry::formatted);
		return registry;
	}

}
