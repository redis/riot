package com.redis.riot;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import org.springframework.batch.core.Job;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemWriter;
import org.springframework.core.io.WritableResource;

import com.redis.riot.core.RiotException;
import com.redis.riot.file.FileType;
import com.redis.riot.file.FileUtils;
import com.redis.riot.file.FileWriterArgs;
import com.redis.riot.file.FileWriterFactory;
import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.common.KeyValue;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "file-export", description = "Export Redis data to files.")
public class FileExport extends AbstractExportCommand<FileExportExecutionContext> {

	public static final FileType DEFAULT_FILE_TYPE = FileType.JSONL;

	@Parameters(arity = "0..1", description = "File path or URL. If omitted, export is written to stdout.", paramLabel = "FILE")
	private String file;

	@Option(names = { "-t", "--filetype" }, description = "File type: ${COMPLETION-CANDIDATES}.", paramLabel = "<type>")
	private FileType fileType;

	@ArgGroup(exclusive = false)
	private FileWriterArgs fileWriterArgs = new FileWriterArgs();

	@Option(names = "--content-type", description = "Type of exported content: ${COMPLETION-CANDIDATES}.", paramLabel = "<type>")
	private ContentType contentType;

	@Override
	protected FileExportExecutionContext newExecutionContext() {
		return new FileExportExecutionContext();
	}

	@Override
	protected FileExportExecutionContext executionContext() {
		FileExportExecutionContext context = super.executionContext();
		FileWriterFactory factory = new FileWriterFactory();
		factory.setArgs(fileWriterArgs);
		context.setFileWriterFactory(factory);
		return context;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected Job job(FileExportExecutionContext context) {
		WritableResource resource;
		try {
			resource = fileWriterArgs.resource(file);
		} catch (IOException e) {
			throw new RiotException("Could not open file " + file, e);
		}
		FileType fileType = fileType(resource);
		ItemWriter writer = context.getFileWriterFactory().create(resource, fileType,
				() -> headerRecord(context, fileType));
		return job(context, step(context, writer).processor(processor(fileType)));
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
	private Map<String, Object> headerRecord(FileExportExecutionContext context, FileType fileType) {
		RedisItemReader<String, String, Object> reader = RedisItemReader.struct();
		configure(context, reader);
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
