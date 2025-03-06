package com.redis.riot;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.springframework.batch.core.Job;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemWriter;
import org.springframework.core.io.WritableResource;
import org.springframework.util.Assert;
import org.springframework.util.MimeType;

import com.redis.riot.core.RiotException;
import com.redis.riot.core.Step;
import com.redis.riot.file.FileWriterRegistry;
import com.redis.riot.file.ResourceFactory;
import com.redis.riot.file.ResourceMap;
import com.redis.riot.file.RiotResourceMap;
import com.redis.riot.file.StdOutProtocolResolver;
import com.redis.riot.file.WriteOptions;
import com.redis.riot.file.WriterFactory;
import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.common.KeyValue;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

public abstract class AbstractFileExport extends AbstractRedisExportCommand {

	@Parameters(arity = "0..1", description = "File path or URL. If omitted, export is written to stdout.", paramLabel = "FILE")
	private String file = StdOutProtocolResolver.DEFAULT_FILENAME;

	@ArgGroup(exclusive = false)
	private FileWriterArgs fileWriterArgs = new FileWriterArgs();

	@Option(names = "--content-type", description = "Type of exported content: ${COMPLETION-CANDIDATES}.", paramLabel = "<type>")
	private ContentType contentType = ContentType.STRUCT;

	private FileWriterRegistry writerRegistry;
	private ResourceFactory resourceFactory;
	private ResourceMap resourceMap;
	private WriteOptions writeOptions;

	@Override
	protected void initialize() {
		super.initialize();
		writerRegistry = writerRegistry();
		resourceFactory = resourceFactory();
		resourceMap = resourceMap();
		writeOptions = writeOptions();
	}

	protected RiotResourceMap resourceMap() {
		return RiotResourceMap.defaultResourceMap();
	}

	protected FileWriterRegistry writerRegistry() {
		return FileWriterRegistry.defaultWriterRegistry();
	}

	protected ResourceFactory resourceFactory() {
		ResourceFactory factory = new ResourceFactory();
		factory.addProtocolResolver(new StdOutProtocolResolver());
		return factory;
	}

	private WriteOptions writeOptions() {
		WriteOptions writeOptions = fileWriterArgs.fileWriterOptions();
		writeOptions.setContentType(getFileType());
		writeOptions.setHeaderSupplier(this::headerRecord);
		return writeOptions;
	}

	@Override
	protected Job job() {
		return job(step());
	}

	protected abstract MimeType getFileType();

	@SuppressWarnings("unchecked")
	private Step<?, ?> step() {
		WritableResource resource;
		try {
			resource = resourceFactory.writableResource(file, writeOptions);
		} catch (IOException e) {
			throw new RiotException(String.format("Could not create resource from file %s", file), e);
		}
		MimeType type = writeOptions.getContentType() == null ? resourceMap.getContentTypeFor(resource)
				: writeOptions.getContentType();
		WriterFactory writerFactory = writerRegistry.getWriterFactory(type);
		Assert.notNull(writerFactory, String.format("No writer found for file %s", file));
		ItemWriter<?> writer = writerFactory.create(resource, writeOptions);
		return step(writer).processor(processor(type));
	}

	@Override
	protected boolean shouldShowProgress() {
		return super.shouldShowProgress() && file != null;
	}

	protected boolean isFlatFile(MimeType type) {
		return ResourceMap.CSV.equals(type) || ResourceMap.PSV.equals(type) || ResourceMap.TSV.equals(type)
				|| ResourceMap.TEXT.equals(type);
	}

	@SuppressWarnings("rawtypes")
	private ItemProcessor processor(MimeType type) {
		if (isFlatFile(type) || contentType == ContentType.MAP) {
			return mapProcessor();
		}
		return null;
	}

	private Map<String, Object> headerRecord() {
		RedisItemReader<String, String> reader = RedisItemReader.struct();
		configureSourceRedisReader(reader);
		try {
			reader.open(new ExecutionContext());
			try {
				KeyValue<String> keyValue = reader.read();
				if (keyValue == null) {
					return Collections.emptyMap();
				}
				return ((ItemProcessor<KeyValue<String>, Map<String, Object>>) mapProcessor()).process(keyValue);
			} catch (Exception e) {
				throw new ItemStreamException("Could not read header record", e);
			}
		} finally {
			reader.close();
		}
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

	public void setWriterRegistry(FileWriterRegistry registry) {
		this.writerRegistry = registry;
	}

}
