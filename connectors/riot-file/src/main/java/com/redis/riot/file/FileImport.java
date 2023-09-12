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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.SimpleJobBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
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

import com.redis.riot.core.AbstractMapImport;
import com.redis.riot.core.RiotExecutionContext;
import com.redis.riot.core.StepBuilder;
import com.redis.riot.core.function.MapToFieldFunction;
import com.redis.riot.core.function.RegexNamedGroupFunction;
import com.redis.riot.core.function.ToMapFunction;
import com.redis.spring.batch.util.BatchUtils;

public class FileImport extends AbstractMapImport {

    public static final String DEFAULT_CONTINUATION_STRING = "\\";

    public static final Character DEFAULT_QUOTE_CHARACTER = DelimitedLineTokenizer.DEFAULT_QUOTE_CHARACTER;

    private final Logger log = LoggerFactory.getLogger(FileImport.class);

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
    protected Job job(RiotExecutionContext executionContext) {
        Iterator<Step> steps = FileUtils.inputResources(files, fileOptions).stream().map(r -> step(executionContext, r))
                .iterator();
        if (!steps.hasNext()) {
            throw new IllegalArgumentException("No file found");
        }
        SimpleJobBuilder job = jobBuilder().start(steps.next());
        while (steps.hasNext()) {
            job.next(steps.next());
        }
        return job.build();
    }

    @SuppressWarnings("unchecked")
    private Step step(RiotExecutionContext executionContext, Resource resource) {
        ItemReader<Map<String, Object>> reader = reader(resource);
        if (maxItemCount != null && reader instanceof AbstractItemCountingItemStreamItemReader) {
            ((AbstractItemCountingItemStreamItemReader<Map<String, Object>>) reader).setMaxItemCount(maxItemCount);
        }
        String name = resource.getDescription();
        StepBuilder<Map<String, Object>, Map<String, Object>> step = createStep();
        step.name(name);
        step.reader(reader);
        step.writer(writer(executionContext));
        step.processor(processor(executionContext));
        step.skippableExceptions(ParseException.class);
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
                log.info("Creating delimited reader for {}", resource);
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
                log.info("Creating fixed-width reader for {}", resource);
                return flatFileReader(resource, fixedLengthTokenizer);
            case XML:
                log.info("Creating XML reader for {}", resource);
                return FileUtils.xmlReader(resource, Map.class);
            case JSON:
                log.info("Creating JSON reader for {}", resource);
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
        builder.strict(true);
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
        return FileUtils.expandAll(Arrays.asList(files)).map(f -> FileUtils.inputResource(f, fileOptions)).map(this::reader);
    }

    /**
     * 
     * @param file the file to create a reader for (can be CSV, Fixed-width, JSON, XML)
     * @return Reader for the given file
     * @throws Exception
     */
    public ItemReader<Map<String, Object>> reader(String file) {
        return reader(FileUtils.inputResource(file, fileOptions));
    }

    @Override
    protected ItemProcessor<Map<String, Object>, Map<String, Object>> processor(RiotExecutionContext executionContext) {
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
        return BatchUtils.processor(processor, regexProcessor);

    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private Function<Map<String, Object>, Map<String, Object>> toFieldFunction(String key, Pattern pattern) {
        return new MapToFieldFunction(key).andThen((Function) new RegexNamedGroupFunction(pattern));
    }

}
