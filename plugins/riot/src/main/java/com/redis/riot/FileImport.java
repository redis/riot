package com.redis.riot;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;

import org.springframework.batch.core.Job;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.function.FunctionItemProcessor;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import com.redis.riot.core.RiotException;
import com.redis.riot.core.RiotUtils;
import com.redis.riot.core.Step;
import com.redis.riot.core.processor.RegexNamedGroupFunction;
import com.redis.riot.file.FileReaderArgs;
import com.redis.riot.file.FileReaderFactory;
import com.redis.riot.file.FileType;
import com.redis.riot.file.FileUtils;
import com.redis.riot.file.MapToFieldFunction;
import com.redis.riot.file.ToMapFunction;
import com.redis.spring.batch.item.redis.RedisItemWriter;
import com.redis.spring.batch.item.redis.common.KeyValue;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "file-import", description = "Import data from files.")
public class FileImport extends AbstractImportCommand<FileImportExecutionContext> {

	@Parameters(arity = "1..*", description = "Files or URLs to import. Use '-' to read from stdin.", paramLabel = "FILE")
	private List<String> files;

	@Option(names = { "-t", "--filetype" }, description = "File type: ${COMPLETION-CANDIDATES}.", paramLabel = "<type>")
	private FileType fileType;

	@ArgGroup(exclusive = false)
	private FileReaderArgs fileReaderArgs = new FileReaderArgs();

	@Option(arity = "1..*", names = "--regex", description = "Regular expressions used to extract values from fields in the form field1=\"regex\" field2=\"regex\"...", paramLabel = "<f=rex>")
	private Map<String, Pattern> regexes = new LinkedHashMap<>();

	@Override
	protected FileImportExecutionContext newExecutionContext() {
		return new FileImportExecutionContext();
	}

	@Override
	protected FileImportExecutionContext executionContext() {
		FileImportExecutionContext context = super.executionContext();
		FileReaderFactory factory = new FileReaderFactory();
		factory.addDeserializer(KeyValue.class, new KeyValueDeserializer());
		factory.setArgs(fileReaderArgs);
		context.setFileReaderFactory(factory);
		return context;
	}

	@Override
	protected Job job(FileImportExecutionContext context) {
		Assert.notEmpty(files, "No file specified");
		List<Step<?, ?>> steps = new ArrayList<>();
		List<Resource> resources = new ArrayList<>();
		for (String file : files) {
			try {
				for (String expandedFile : FileUtils.expand(file)) {
					resources.add(fileReaderArgs.resource(expandedFile));
				}
			} catch (FileNotFoundException e) {
				throw new RiotException("File not found: " + file);
			} catch (IOException e) {
				throw new RiotException("Could not read file " + file, e);
			}
		}
		for (Resource resource : resources) {
			Step<?, ?> step = step(context, resource);
			step.skip(ParseException.class);
			step.skip(org.springframework.batch.item.ParseException.class);
			step.noRetry(ParseException.class);
			step.noRetry(org.springframework.batch.item.ParseException.class);
			step.taskName(taskName(resource));
			steps.add(step);
		}
		return job(context, steps);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private Step<?, ?> step(FileImportExecutionContext context, Resource resource) {
		String name = resource.getFilename();
		FileType type = fileType(resource);
		if (hasOperations()) {
			ItemReader<Map<String, Object>> reader = (ItemReader) context.getFileReaderFactory().createReader(resource,
					type, Map.class);
			RedisItemWriter<String, String, Map<String, Object>> writer = operationWriter();
			configure(context, writer);
			return new Step<>(name, reader, writer).processor(processor(context));
		}
		Assert.isTrue(type != FileType.CSV, "CSV file import requires a Redis command");
		Assert.isTrue(type != FileType.FIXED, "Fixed-length file import requires a Redis command");
		ItemReader<KeyValue> reader = context.getFileReaderFactory().createReader(resource, type, KeyValue.class);
		RedisItemWriter<String, String, KeyValue<String, Object>> writer = RedisItemWriter.struct();
		configure(context, writer);
		return new Step<>(name, reader, writer);
	}

	@Override
	protected ItemProcessor<Map<String, Object>, Map<String, Object>> processor(FileImportExecutionContext context) {
		return RiotUtils.processor(super.processor(context), regexProcessor());
	}

	private FileType fileType(Resource resource) {
		if (fileType == null) {
			FileType type = FileUtils.fileType(resource);
			Assert.notNull(type, String.format("Unknown type for file %s", resource.getFilename()));
			return type;
		}
		return fileType;
	}

	private String taskName(Resource resource) {
		return String.format("Importing %s", resource.getFilename());
	}

	private ItemProcessor<Map<String, Object>, Map<String, Object>> regexProcessor() {
		if (CollectionUtils.isEmpty(regexes)) {
			return null;
		}
		List<Function<Map<String, Object>, Map<String, Object>>> functions = new ArrayList<>();
		functions.add(Function.identity());
		regexes.entrySet().stream().map(e -> toFieldFunction(e.getKey(), e.getValue())).forEach(functions::add);
		return new FunctionItemProcessor<>(new ToMapFunction<>(functions));
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private Function<Map<String, Object>, Map<String, Object>> toFieldFunction(String key, Pattern regex) {
		return new MapToFieldFunction(key).andThen((Function) new RegexNamedGroupFunction(regex));
	}

	public List<String> getFiles() {
		return files;
	}

	public void setFiles(String... files) {
		setFiles(Arrays.asList(files));
	}

	public void setFiles(List<String> files) {
		this.files = files;
	}

	public FileReaderArgs getFileReaderArgs() {
		return fileReaderArgs;
	}

	public void setFileReaderArgs(FileReaderArgs args) {
		this.fileReaderArgs = args;
	}

	public Map<String, Pattern> getRegexes() {
		return regexes;
	}

	public void setRegexes(Map<String, Pattern> regexes) {
		this.regexes = regexes;
	}

	public FileType getFileType() {
		return fileType;
	}

	public void setFileType(FileType fileType) {
		this.fileType = fileType;
	}

}
