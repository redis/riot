package com.redis.riot;

import java.util.Collections;
import java.util.Map;

import org.springframework.batch.core.Job;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemWriter;

import com.redis.riot.file.FileWriterArgs;
import com.redis.riot.file.FileWriterFactory;
import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.common.KeyValue;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "file-export", description = "Export Redis data to files.")
public class FileExport extends AbstractExportCommand {

	@ArgGroup(exclusive = false)
	private FileWriterArgs fileWriterArgs = new FileWriterArgs();

	@Option(names = "--content-type", description = "Type of exported content: ${COMPLETION-CANDIDATES}.", paramLabel = "<type>")
	private ContentType contentType;

	@SuppressWarnings("unchecked")
	@Override
	protected Job job() {
		return job(step(writer()).processor(processor()));
	}

	@Override
	protected boolean shouldShowProgress() {
		return super.shouldShowProgress() && fileWriterArgs.getFile() != null;
	}

	public ContentType contentType() {
		switch (fileWriterArgs.fileType()) {
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

	private ItemProcessor<KeyValue<String, Object>, ?> processor() {
		if (contentType() == ContentType.STRUCT) {
			return keyValueProcessor();
		}
		return mapProcessor();
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> headerRecord() {
		RedisItemReader<String, String, Object> reader = RedisItemReader.struct();
		configure(reader);
		try {
			reader.open(new ExecutionContext());
			try {
				KeyValue<String, Object> keyValue = reader.read();
				if (keyValue == null) {
					return Collections.emptyMap();
				}
				return ((ItemProcessor<KeyValue<String, Object>, Map<String, Object>>) processor()).process(keyValue);
			} catch (Exception e) {
				throw new ItemStreamException("Could not read header record", e);
			}
		} finally {
			reader.close();
		}
	}

	@SuppressWarnings("rawtypes")
	private ItemWriter writer() {
		FileWriterFactory factory = new FileWriterFactory();
		factory.setArgs(fileWriterArgs);
		factory.setHeaderSupplier(this::headerRecord);
		return factory.create(fileWriterArgs.getFile());
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

}
