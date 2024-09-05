package com.redis.riot;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import org.springframework.batch.core.Job;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.transform.LineAggregator;
import org.springframework.batch.item.file.transform.PassThroughFieldExtractor;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.core.io.WritableResource;
import org.springframework.util.CollectionUtils;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redis.riot.file.FileType;
import com.redis.riot.file.FileUtils;
import com.redis.riot.file.JsonLineAggregator;
import com.redis.riot.file.xml.XmlResourceItemWriter;
import com.redis.riot.file.xml.XmlResourceItemWriterBuilder;
import com.redis.riot.resource.FlatFileItemWriter;
import com.redis.riot.resource.FlatFileItemWriterBuilder;
import com.redis.riot.resource.FlatFileItemWriterBuilder.DelimitedBuilder;
import com.redis.riot.resource.FlatFileItemWriterBuilder.FormattedBuilder;
import com.redis.riot.resource.JsonFileItemWriter;
import com.redis.riot.resource.JsonFileItemWriterBuilder;
import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.common.KeyValue;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "file-export", description = "Export Redis data to files.")
public class FileExport extends AbstractRedisExportCommand {

	public static final FileType DEFAULT_FILE_TYPE = FileType.JSONL;

	@Parameters(arity = "0..1", description = "File path or URL. If omitted, export is written to stdout.", paramLabel = "FILE")
	private String file;

	@Option(names = { "-t", "--filetype" }, description = "File type: ${COMPLETION-CANDIDATES}.", paramLabel = "<type>")
	private FileType fileType;

	@ArgGroup(exclusive = false)
	private FileWriterArgs fileWriterArgs = new FileWriterArgs();

	@Option(names = "--content-type", description = "Type of exported content: ${COMPLETION-CANDIDATES}.", paramLabel = "<type>")
	private ContentType contentType;

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected Job job() throws IOException {
		WritableResource resource = fileWriterArgs.resource(file);
		FileType fileType = fileType(resource);
		ItemWriter writer = create(resource, fileType, () -> headerRecord(fileType));
		return job(step(writer).processor(processor(fileType)));
	}

	private FileType fileType(WritableResource resource) {
		if (fileType == null) {
			return Optional.ofNullable(FileUtils.fileType(resource)).orElse(DEFAULT_FILE_TYPE);
		}
		return fileType;
	}

	@Override
	protected boolean shouldShowProgress() {
		return super.shouldShowProgress() && file != null;
	}

	private ContentType contentType(FileType fileType) {
		switch (fileType) {
		case CSV:
		case FIXED:
			return ContentType.MAP;
		default:
			if (contentType == null) {
				return ContentType.STRUCT;
			}
			return contentType;
		}
	}

	private ItemProcessor<KeyValue<String, Object>, ?> processor(FileType fileType) {
		if (contentType(fileType) == ContentType.MAP) {
			return mapProcessor();
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> headerRecord(FileType fileType) {
		RedisItemReader<String, String, Object> reader = RedisItemReader.struct();
		configureSourceRedisReader(reader);
		try {
			reader.open(new ExecutionContext());
			try {
				KeyValue<String, Object> keyValue = reader.read();
				if (keyValue == null) {
					return Collections.emptyMap();
				}
				return ((ItemProcessor<KeyValue<String, Object>, Map<String, Object>>) processor(fileType))
						.process(keyValue);
			} catch (Exception e) {
				throw new ItemStreamException("Could not read header record", e);
			}
		} finally {
			reader.close();
		}
	}

	@SuppressWarnings("unchecked")
	public <T> ItemWriter<T> create(WritableResource resource, FileType fileType,
			Supplier<Map<String, Object>> headerSupplier) {
		switch (fileType) {
		case CSV:
			return (ItemWriter<T>) delimitedWriter(resource, headerSupplier);
		case FIXED:
			return (ItemWriter<T>) fixedLengthWriter(resource, headerSupplier);
		case JSON:
			return jsonWriter(resource);
		case JSONL:
			return jsonlWriter(resource);
		case XML:
			return xmlWriter(resource);
		default:
			throw new UnsupportedOperationException("Unsupported file type: " + fileType);
		}
	}

	private <T> FlatFileItemWriter<T> jsonlWriter(WritableResource resource) {
		FlatFileItemWriterBuilder<T> builder = flatFileWriter(resource);
		builder.lineAggregator(new JsonLineAggregator<>(objectMapper(new ObjectMapper())));
		return builder.build();
	}

	private <T extends ObjectMapper> T objectMapper(T objectMapper) {
		objectMapper.setSerializationInclusion(Include.NON_NULL);
		objectMapper.setSerializationInclusion(Include.NON_DEFAULT);
		return objectMapper;
	}

	private <T> JsonFileItemWriter<T> jsonWriter(WritableResource resource) {
		JsonFileItemWriterBuilder<T> writer = new JsonFileItemWriterBuilder<>();
		writer.name(resource.getFilename());
		writer.append(fileWriterArgs.isAppend());
		writer.encoding(fileWriterArgs.getFileArgs().getEncoding());
		writer.lineSeparator(fileWriterArgs.getLineSeparator());
		writer.resource(resource);
		writer.saveState(false);
		ObjectMapper mapper = objectMapper(new ObjectMapper());
		writer.jsonObjectMarshaller(new JacksonJsonObjectMarshaller<>(mapper));
		return writer.build();
	}

	private <T> XmlResourceItemWriter<T> xmlWriter(WritableResource resource) {
		XmlResourceItemWriterBuilder<T> writer = new XmlResourceItemWriterBuilder<>();
		writer.name(resource.getFilename());
		writer.append(fileWriterArgs.isAppend());
		writer.encoding(fileWriterArgs.getFileArgs().getEncoding());
		writer.lineSeparator(fileWriterArgs.getLineSeparator());
		writer.rootName(fileWriterArgs.getRootName());
		writer.resource(resource);
		writer.saveState(false);
		XmlMapper mapper = objectMapper(new XmlMapper());
		mapper.setConfig(mapper.getSerializationConfig().withRootName(fileWriterArgs.getElementName()));
		writer.xmlObjectMarshaller(new JacksonJsonObjectMarshaller<>(mapper));
		return writer.build();
	}

	private ItemWriter<Map<String, Object>> delimitedWriter(WritableResource resource,
			Supplier<Map<String, Object>> headerSupplier) {
		FlatFileItemWriterBuilder<Map<String, Object>> writer = flatFileWriter(resource);
		DelimitedBuilder<Map<String, Object>> delimitedBuilder = writer.delimited();
		delimitedBuilder.delimiter(fileWriterArgs.getFileArgs().getDelimiter());
		delimitedBuilder.fieldExtractor(new PassThroughFieldExtractor<>());
		delimitedBuilder.quoteCharacter(String.valueOf(fileWriterArgs.getFileArgs().getQuoteCharacter()));
		return writer(writer, delimitedBuilder.build(), headerSupplier);
	}

	private FlatFileItemWriter<Map<String, Object>> writer(FlatFileItemWriterBuilder<Map<String, Object>> writer,
			LineAggregator<Map<String, Object>> lineAggregator, Supplier<Map<String, Object>> headerSupplier) {
		writer.lineAggregator(lineAggregator);
		if (fileWriterArgs.getFileArgs().isHeader()) {
			Map<String, Object> headerRecord = headerSupplier.get();
			if (CollectionUtils.isEmpty(headerRecord)) {
				log.warn("Could not determine header");
			} else {
				List<String> fields = new ArrayList<>(headerRecord.keySet());
				Collections.sort(fields);
				Map<String, Object> fieldMap = new LinkedHashMap<>();
				fields.forEach(f -> fieldMap.put(f, f));
				String headerLine = lineAggregator.aggregate(fieldMap);
				log.info("Found header: {}", headerLine);
				writer.headerCallback(w -> w.write(headerLine));
			}
		}
		return writer.build();
	}

	private ItemWriter<Map<String, Object>> fixedLengthWriter(WritableResource resource,
			Supplier<Map<String, Object>> headerSupplier) {
		FlatFileItemWriterBuilder<Map<String, Object>> writer = flatFileWriter(resource);
		FormattedBuilder<Map<String, Object>> formattedBuilder = writer.formatted();
		formattedBuilder.format(fileWriterArgs.getFormatterString());
		formattedBuilder.fieldExtractor(new PassThroughFieldExtractor<>());
		return writer(writer, formattedBuilder.build(), headerSupplier);
	}

	private <T> FlatFileItemWriterBuilder<T> flatFileWriter(WritableResource resource) {
		FlatFileItemWriterBuilder<T> builder = new FlatFileItemWriterBuilder<>();
		builder.name(resource.getFilename());
		builder.resource(resource);
		builder.append(fileWriterArgs.isAppend());
		builder.encoding(fileWriterArgs.getFileArgs().getEncoding());
		builder.forceSync(fileWriterArgs.isForceSync());
		builder.lineSeparator(fileWriterArgs.getLineSeparator());
		builder.saveState(false);
		builder.shouldDeleteIfEmpty(fileWriterArgs.isShouldDeleteIfEmpty());
		builder.shouldDeleteIfExists(fileWriterArgs.isShouldDeleteIfExists());
		builder.transactional(fileWriterArgs.isTransactional());
		return builder;
	}

	public String getFile() {
		return file;
	}

	public void setFile(String file) {
		this.file = file;
	}

	public FileWriterArgs getFileWriterArgs() {
		return fileWriterArgs;
	}

	public void setFileWriterArgs(FileWriterArgs fileWriterArgs) {
		this.fileWriterArgs = fileWriterArgs;
	}

	public ContentType getContentType() {
		return contentType;
	}

	public void setContentType(ContentType contentType) {
		this.contentType = contentType;
	}

	public FileType getFileType() {
		return fileType;
	}

	public void setFileType(FileType fileType) {
		this.fileType = fileType;
	}

}
