package com.redis.riot;

import java.util.Collections;
import java.util.Map;

import org.springframework.batch.core.Job;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemWriter;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import com.redis.riot.core.EvaluationContextArgs;
import com.redis.riot.file.FileWriterArgs;
import com.redis.riot.file.FileWriterFactory;
import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.common.KeyValue;
import com.redis.spring.batch.item.redis.reader.MemKeyValue;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "file-export", description = "Export Redis data to files.")
public class FileExport extends AbstractExport {

	@ArgGroup(exclusive = false)
	private FileWriterArgs fileWriterArgs = new FileWriterArgs();

	@Option(names = "--content-type", description = "Type of exported content: ${COMPLETION-CANDIDATES}.", paramLabel = "<type>")
	private ContentType contentType;

	@ArgGroup(exclusive = false)
	private EvaluationContextArgs evaluationContextArgs = new EvaluationContextArgs();

	@ArgGroup(exclusive = false)
	private KeyValueMapProcessorArgs processorArgs = new KeyValueMapProcessorArgs();

	public void copyTo(FileExport target) {
		super.copyTo(target);
		target.fileWriterArgs = fileWriterArgs;
		target.contentType = contentType;
		target.processorArgs = processorArgs;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected Job job() {
		return job(exportStep(reader(), writer()).processor(processor()).taskName("Exporting"));
	}

	private RedisItemReader<String, String, MemKeyValue<String, Object>> reader() {
		RedisItemReader<String, String, MemKeyValue<String, Object>> reader = RedisItemReader.struct();
		configure(reader);
		return reader;
	}

	@Override
	protected boolean shouldShowProgress() {
		return super.shouldShowProgress() && fileWriterArgs.getFile() != null;
	}

	public ContentType contentType() {
		if (contentType == null) {
			switch (fileWriterArgs.fileType()) {
			case CSV:
			case FIXED:
				return ContentType.FLAT;
			default:
				return ContentType.REDIS;
			}
		}
		return contentType;
	}

	private ItemProcessor<KeyValue<String, Object>, ?> processor() {
		StandardEvaluationContext evaluationContext = evaluationContext(evaluationContextArgs);
		if (contentType() == ContentType.REDIS) {
			return processorArgs.getKeyValueProcessorArgs().processor(evaluationContext);
		}
		return processorArgs.processor(evaluationContext);
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> headerRecord() {
		RedisItemReader<String, String, MemKeyValue<String, Object>> reader = reader();
		try {
			reader.open(new ExecutionContext());
			try {
				MemKeyValue<String, Object> keyValue = reader.read();
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
		factory.setOptions(fileWriterArgs);
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

	public KeyValueMapProcessorArgs getProcessorArgs() {
		return processorArgs;
	}

	public void setProcessorArgs(KeyValueMapProcessorArgs mapProcessorArgs) {
		this.processorArgs = mapProcessorArgs;
	}

}
