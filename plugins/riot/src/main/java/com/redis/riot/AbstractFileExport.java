package com.redis.riot;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.step.builder.StepBuilderException;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemWriter;

import com.redis.riot.core.Step;
import com.redis.riot.file.FileType;
import com.redis.riot.file.Files;
import com.redis.riot.file.FileWriterOptions;
import com.redis.riot.file.FileWriterRegistry;
import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.common.KeyValue;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "file-export", description = "Export Redis data to files.")
public abstract class AbstractFileExport extends AbstractRedisExportCommand {

	private FileWriterRegistry writerRegistry = Files.writerRegistry;

	@Parameters(arity = "0..1", description = "File path or URL. If omitted, export is written to stdout.", paramLabel = "FILE")
	private String file;

	@ArgGroup(exclusive = false)
	private FileWriterArgs fileWriterArgs = new FileWriterArgs();

	@Option(names = "--content-type", description = "Type of exported content: ${COMPLETION-CANDIDATES}.", paramLabel = "<type>")
	private ContentType contentType = ContentType.STRUCT;

	@Override
	protected Job job() {
		return job(step());
	}

	protected abstract FileType getFileType();

	@SuppressWarnings("unchecked")
	private Step<?, ?> step() {
		FileWriterOptions writerOptions = fileWriterArgs.fileWriterOptions();
		writerOptions.getFileOptions().setFileType(getFileType());
		writerOptions.setHeaderSupplier(this::headerRecord);
		ItemWriter<?> writer;
		try {
			writer = writerRegistry.writer(file, writerOptions);
		} catch (IOException e) {
			throw new StepBuilderException(e);
		}
		return step(writer).processor(processor());
	}

	@Override
	protected boolean shouldShowProgress() {
		return super.shouldShowProgress() && file != null;
	}

	private ContentType contentType() {
		String extension = Files.extension(file);
		if (FileType.DELIMITED.supportsExtension(extension) || FileType.FIXED_WIDTH.supportsExtension(extension)) {
			return ContentType.MAP;
		}
		return contentType;
	}

	@SuppressWarnings("rawtypes")
	private ItemProcessor processor() {
		if (contentType() == ContentType.MAP) {
			return mapProcessor();
		}
		return null;
	}

	@SuppressWarnings("unchecked")
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
				return ((ItemProcessor<KeyValue<String>, Map<String, Object>>) processor()).process(keyValue);
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
