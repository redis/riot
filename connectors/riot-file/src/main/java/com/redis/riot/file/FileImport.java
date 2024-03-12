package com.redis.riot.file;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.job.builder.SimpleJobBuilder;
import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.separator.DefaultRecordSeparatorPolicy;
import org.springframework.batch.item.file.separator.RecordSeparatorPolicy;
import org.springframework.batch.item.file.transform.AbstractLineTokenizer;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.FixedLengthTokenizer;
import org.springframework.batch.item.file.transform.Range;
import org.springframework.batch.item.file.transform.RangeArrayPropertyEditor;
import org.springframework.batch.item.function.FunctionItemProcessor;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;

import com.redis.riot.core.AbstractImport;
import com.redis.riot.core.RiotContext;
import com.redis.riot.core.RiotUtils;
import com.redis.riot.core.function.MapToFieldFunction;
import com.redis.riot.core.function.RegexNamedGroupFunction;
import com.redis.riot.core.function.ToMapFunction;

public class FileImport extends AbstractImport {

	public static final String DEFAULT_CONTINUATION_STRING = "\\";
	public static final Character DEFAULT_QUOTE_CHARACTER = DelimitedLineTokenizer.DEFAULT_QUOTE_CHARACTER;
	private static final String PIPE_DELIMITER = "|";

	private List<String> files;
	private FileOptions fileOptions = new FileOptions();
	private FileType fileType;
	private Integer maxItemCount;
	private List<String> fields;
	private boolean header;
	private Integer headerLine;
	private String delimiter;
	private Integer linesToSkip;
	private int[] includedFields;
	private List<String> columnRanges;
	private Character quoteCharacter = DEFAULT_QUOTE_CHARACTER;
	private String continuationString = DEFAULT_CONTINUATION_STRING;
	private Map<String, Pattern> regexes;

	public void setFiles(String... files) {
		setFiles(Arrays.asList(files));
	}

	public void setFiles(List<String> files) {
		Assert.notEmpty(files, "No file specified");
		this.files = files;
	}

	public void setRegexes(Map<String, Pattern> regexes) {
		this.regexes = regexes;
	}

	public void setFileOptions(FileOptions fileOptions) {
		this.fileOptions = fileOptions;
	}

	public void setFileType(FileType fileType) {
		this.fileType = fileType;
	}

	public void setMaxItemCount(Integer maxItemCount) {
		this.maxItemCount = maxItemCount;
	}

	public void setFields(List<String> names) {
		this.fields = names;
	}

	public void setHeader(boolean header) {
		this.header = header;
	}

	public void setDelimiter(String delimiter) {
		this.delimiter = delimiter;
	}

	public void setHeaderLine(Integer index) {
		this.headerLine = index;
	}

	public void setLinesToSkip(Integer linesToSkip) {
		this.linesToSkip = linesToSkip;
	}

	public void setIncludedFields(int... indexes) {
		this.includedFields = indexes;
	}

	public void setColumnRanges(List<String> columnRanges) {
		this.columnRanges = columnRanges;
	}

	public void setQuoteCharacter(Character quoteCharacter) {
		this.quoteCharacter = quoteCharacter;
	}

	public void setContinuationString(String continuationString) {
		this.continuationString = continuationString;
	}

	@Override
	protected Job job(RiotContext context) throws Exception {
		List<Resource> resources = FileUtils.inputResources(files, fileOptions);
		if (resources.isEmpty()) {
			throw new IllegalArgumentException("No file found");
		}
		List<TaskletStep> steps = new ArrayList<>();
		for (Resource resource : resources) {
			steps.add(step(context, resource));
		}
		Iterator<TaskletStep> iterator = steps.iterator();
		SimpleJobBuilder job = jobBuilder().start(iterator.next());
		while (iterator.hasNext()) {
			job.next(iterator.next());
		}
		return job.build();
	}

	private TaskletStep step(RiotContext context, Resource resource) throws Exception {
		ItemReader<Map<String, Object>> reader = reader(resource);
		if (maxItemCount != null && reader instanceof AbstractItemCountingItemStreamItemReader) {
			((AbstractItemCountingItemStreamItemReader<Map<String, Object>>) reader).setMaxItemCount(maxItemCount);
		}
		ItemProcessor<Map<String, Object>, Map<String, Object>> processor = processor(context);
		ItemWriter<Map<String, Object>> writer = writer(context);
		FaultTolerantStepBuilder<Map<String, Object>, Map<String, Object>> step = step(resource.getFilename(), reader,
				processor, writer);
		step.skip(ParseException.class);
		step.noRetry(ParseException.class);
		return step.build();
	}

	private ItemReader<Map<String, Object>> reader(Resource resource) {
		FileType type = type(resource);
		switch (type) {
		case DELIMITED:
			DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
			tokenizer.setDelimiter(delimiter(resource));
			tokenizer.setQuoteCharacter(quoteCharacter);
			if (!ObjectUtils.isEmpty(includedFields)) {
				tokenizer.setIncludedFields(includedFields);
			}
			return flatFileReader(resource, tokenizer);
		case FIXED:
			FixedLengthTokenizer fixedLengthTokenizer = new FixedLengthTokenizer();
			RangeArrayPropertyEditor editor = new RangeArrayPropertyEditor();
			Assert.notEmpty(columnRanges, "Column ranges are required");
			editor.setAsText(String.join(",", columnRanges));
			Range[] ranges = (Range[]) editor.getValue();
			if (ranges.length == 0) {
				throw new IllegalArgumentException("Invalid ranges specified: " + columnRanges);
			}
			fixedLengthTokenizer.setColumns(ranges);
			return flatFileReader(resource, fixedLengthTokenizer);
		case XML:
			return FileUtils.xmlReader(resource, Map.class);
		case JSON:
			return FileUtils.jsonReader(resource, Map.class);
		default:
			throw new UnsupportedOperationException("Unsupported file type: " + type);
		}
	}

	private String delimiter(Resource resource) {
		if (delimiter != null) {
			return delimiter;
		}
		switch (FileUtils.extension(resource)) {
		case CSV:
			return DelimitedLineTokenizer.DELIMITER_COMMA;
		case PSV:
			return PIPE_DELIMITER;
		case TSV:
			return DelimitedLineTokenizer.DELIMITER_TAB;
		default:
			throw new IllegalArgumentException("Unknown file extension for " + resource);
		}
	}

	private FileType type(Resource resource) {
		if (fileType != null) {
			return fileType;
		}
		FileExtension extension = FileUtils.extension(resource);
		switch (extension) {
		case FW:
			return FileType.FIXED;
		case JSON:
			return FileType.JSON;
		case XML:
			return FileType.XML;
		case CSV:
		case PSV:
		case TSV:
			return FileType.DELIMITED;
		default:
			throw new UnknownFileTypeException("Unknown file extension: " + extension);
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private <T extends Map<String, Object>> FlatFileItemReader<T> flatFileReader(Resource resource,
			AbstractLineTokenizer tokenizer) {
		if (!ObjectUtils.isEmpty(fields)) {
			tokenizer.setNames(fields.toArray(new String[0]));
		}
		FlatFileItemReaderBuilder<T> builder = new FlatFileItemReaderBuilder<>();
		builder.resource(resource);
		if (fileOptions.getEncoding() != null) {
			builder.encoding(fileOptions.getEncoding());
		}
		builder.lineTokenizer(tokenizer);
		builder.recordSeparatorPolicy(recordSeparatorPolicy());
		builder.linesToSkip(linesToSkip());
		builder.saveState(false);
		builder.fieldSetMapper((FieldSetMapper) new MapFieldSetMapper());
		builder.skippedLinesCallback(new HeaderCallbackHandler(tokenizer, headerIndex()));
		return builder.build();
	}

	private RecordSeparatorPolicy recordSeparatorPolicy() {
		return new DefaultRecordSeparatorPolicy(quoteCharacter.toString(), continuationString);
	}

	private int headerIndex() {
		if (headerLine != null) {
			return headerLine;
		}
		return linesToSkip() - 1;
	}

	private int linesToSkip() {
		if (linesToSkip != null) {
			return linesToSkip;
		}
		if (header) {
			return 1;
		}
		return 0;
	}

	/**
	 * 
	 * @param files Files to create readers for
	 * @return List of readers for the given files, which might contain wildcards
	 * @throws IOException
	 */
	public Stream<ItemReader<Map<String, Object>>> readers(String... files) {
		return FileUtils.expandAll(Arrays.asList(files)).map(f -> FileUtils.safeInputResource(f, fileOptions))
				.map(this::reader);
	}

	/**
	 * 
	 * @param file the file to create a reader for (can be CSV, Fixed-width, JSON,
	 *             XML)
	 * @return Reader for the given file
	 * @throws IOException
	 * @throws Exception
	 */
	public ItemReader<Map<String, Object>> reader(String file) throws IOException {
		return reader(FileUtils.inputResource(file, fileOptions));
	}

	@Override
	protected ItemProcessor<Map<String, Object>, Map<String, Object>> processor(RiotContext executionContext) {
		ItemProcessor<Map<String, Object>, Map<String, Object>> processor = super.processor(executionContext);
		if (CollectionUtils.isEmpty(regexes)) {
			return processor;
		}
		List<Function<Map<String, Object>, Map<String, Object>>> regexFunctions = new ArrayList<>();
		regexFunctions.add(Function.identity());
		regexes.entrySet().stream().map(e -> toFieldFunction(e.getKey(), e.getValue())).forEach(regexFunctions::add);
		ToMapFunction<Map<String, Object>, String, Object> function = new ToMapFunction<>(regexFunctions);
		ItemProcessor<Map<String, Object>, Map<String, Object>> regexProcessor = new FunctionItemProcessor<>(function);
		if (processor == null) {
			return regexProcessor;
		}
		return RiotUtils.processor(processor, regexProcessor);

	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private Function<Map<String, Object>, Map<String, Object>> toFieldFunction(String key, Pattern pattern) {
		return new MapToFieldFunction(key).andThen((Function) new RegexNamedGroupFunction(pattern));
	}

}
