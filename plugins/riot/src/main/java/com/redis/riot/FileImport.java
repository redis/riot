package com.redis.riot;

import java.text.MessageFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.function.FunctionItemProcessor;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import com.redis.riot.core.RiotUtils;
import com.redis.riot.core.Step;
import com.redis.riot.core.function.RegexNamedGroupFunction;
import com.redis.riot.file.FileReaderArgs;
import com.redis.riot.file.FileReaderFactory;
import com.redis.riot.file.MapToFieldFunction;
import com.redis.riot.file.ToMapFunction;
import com.redis.spring.batch.item.redis.RedisItemWriter;
import com.redis.spring.batch.item.redis.common.KeyValue;
import com.redis.spring.batch.item.redis.reader.MemKeyValue;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(name = "file-import", description = "Import data from files.")
public class FileImport extends AbstractImport {

	private static final String TASK_NAME_FORMAT = "Importing {0}";

	private final FileReaderFactory factory = new FileReaderFactory();

	@ArgGroup(exclusive = false)
	private FileReaderArgs fileReaderArgs = new FileReaderArgs();

	@ArgGroup(exclusive = false, heading = "Processor options%n")
	private FileImportProcessorArgs processorArgs = new FileImportProcessorArgs();

	public void copyTo(FileImport target) {
		super.copyTo(target);
		target.fileReaderArgs = fileReaderArgs;
		target.processorArgs = processorArgs;
	}

	@Override
	protected void setup() {
		factory.addDeserializer(MemKeyValue.class, new MemKeyValueDeserializer());
		factory.setArgs(fileReaderArgs);
		factory.setItemType(itemType());
		super.setup();
	}

	@Override
	protected Job job() {
		Assert.notEmpty(fileReaderArgs.getFiles(), "No file specified");
		return job(fileReaderArgs.resources().stream().map(this::step).collect(Collectors.toList()));
	}

	@SuppressWarnings("unchecked")
	private Step<?, ?> step(Resource resource) {
		Step<?, ?> step = new Step<>(factory.create(resource), writer());
		step.processor(processor());
		step.name(resource.getFilename());
		step.taskName(MessageFormat.format(TASK_NAME_FORMAT, resource.getFilename()));
		return step;
	}

	@Override
	protected <I, O> FaultTolerantStepBuilder<I, O> faultTolerant(SimpleStepBuilder<I, O> step) {
		FaultTolerantStepBuilder<I, O> faultTolerantStep = super.faultTolerant(step);
		faultTolerantStep.skip(ParseException.class);
		faultTolerantStep.noRetry(ParseException.class);
		return faultTolerantStep;
	}

	@SuppressWarnings("rawtypes")
	private ItemProcessor processor() {
		if (hasOperations()) {
			return mapProcessor();
		}
		return processorArgs.getKeyValueProcessorArgs()
				.processor(evaluationContext(processorArgs.getEvaluationContextArgs()));
	}

	protected ItemProcessor<Map<String, Object>, Map<String, Object>> mapProcessor() {
		ItemProcessor<Map<String, Object>, Map<String, Object>> processor = mapProcessor(processorArgs);
		return RiotUtils.processor(processor, regexProcessor());

	}

	private ItemProcessor<Map<String, Object>, Map<String, Object>> regexProcessor() {
		Map<String, Pattern> regexes = processorArgs.getRegexes();
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

	@SuppressWarnings("rawtypes")
	private ItemWriter writer() {
		if (hasOperations()) {
			return mapWriter();
		}
		RedisItemWriter<String, String, KeyValue<String, Object>> writer = RedisItemWriter.struct();
		configure(writer);
		return writer;
	}

	private Class<?> itemType() {
		if (hasOperations()) {
			return Map.class;
		}
		return MemKeyValue.class;
	}

	public FileReaderArgs getFileReaderArgs() {
		return fileReaderArgs;
	}

	public void setFileReaderArgs(FileReaderArgs args) {
		this.fileReaderArgs = args;
	}

	public FileImportProcessorArgs getProcessorArgs() {
		return processorArgs;
	}

	public void setProcessorArgs(FileImportProcessorArgs args) {
		this.processorArgs = args;
	}

}
