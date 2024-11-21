package com.redis.riot;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.springframework.batch.core.Job;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.util.MimeType;

import com.redis.riot.core.RiotInitializationException;
import com.redis.riot.core.Step;
import com.redis.riot.file.FileUtils;
import com.redis.riot.file.FileWriterRegistry;
import com.redis.riot.file.FileWriterResult;
import com.redis.riot.file.StdOutProtocolResolver;
import com.redis.riot.file.WriteOptions;
import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.common.KeyValue;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "file-export", description = "Export Redis data to files.")
public abstract class AbstractFileExport extends AbstractRedisExportCommand {

	private Set<MimeType> flatFileTypes = new HashSet<>(
			Arrays.asList(FileUtils.CSV, FileUtils.PSV, FileUtils.TSV, FileUtils.TEXT));

	@Parameters(arity = "0..1", description = "File path or URL. If omitted, export is written to stdout.", paramLabel = "FILE")
	private String file = StdOutProtocolResolver.DEFAULT_FILENAME;

	@ArgGroup(exclusive = false)
	private FileWriterArgs fileWriterArgs = new FileWriterArgs();

	@Option(names = "--content-type", description = "Type of exported content: ${COMPLETION-CANDIDATES}.", paramLabel = "<type>")
	private ContentType contentType = ContentType.STRUCT;

	private FileWriterRegistry writerRegistry;
	private WriteOptions writeOptions;

	@Override
	protected void initialize() throws RiotInitializationException {
		super.initialize();
		writerRegistry = FileWriterRegistry.defaultWriterRegistry();
		writeOptions = writeOptions();
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

	public void setFlatFileTypes(MimeType... types) {
		this.flatFileTypes = new HashSet<>(Arrays.asList(types));
	}

	protected abstract MimeType getFileType();

	@SuppressWarnings("unchecked")
	private Step<?, ?> step() {
		FileWriterResult writer = writerRegistry.find(file, writeOptions);
		return step(writer.getWriter()).processor(processor(writer.getType()));
	}

	@Override
	protected boolean shouldShowProgress() {
		return super.shouldShowProgress() && file != null;
	}

	private boolean isFlatFile(MimeType type) {
		return flatFileTypes.contains(type);
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
