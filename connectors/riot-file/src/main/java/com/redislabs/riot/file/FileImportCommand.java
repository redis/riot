package com.redislabs.riot.file;

import com.redislabs.riot.AbstractImportCommand;
import com.redislabs.riot.ProcessorOptions;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.step.builder.FaultTolerantStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.separator.DefaultRecordSeparatorPolicy;
import org.springframework.batch.item.file.separator.RecordSeparatorPolicy;
import org.springframework.batch.item.file.transform.AbstractLineTokenizer;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.FixedLengthTokenizer;
import org.springframework.batch.item.file.transform.Range;
import org.springframework.batch.item.file.transform.RangeArrayPropertyEditor;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;
import org.springframework.batch.item.xml.XmlItemReader;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import picocli.CommandLine;
import picocli.CommandLine.Command;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Slf4j
@Data
@EqualsAndHashCode(callSuper = true)
@Command(name = "import", description = "Import delimited, fixed-width, JSON, or XML files into Redis.")
public class FileImportCommand extends AbstractImportCommand<Map<String, Object>, Map<String, Object>> {

    private enum FileType {
        DELIMITED, FIXED, JSON, XML
    }

    private static final String DELIMITER_PIPE = "|";

    @SuppressWarnings("unused")
    @CommandLine.Parameters(arity = "0..*", description = "One ore more files or URLs", paramLabel = "FILE")
    private List<String> files = new ArrayList<>();
    @CommandLine.Option(names = {"-t", "--filetype"}, description = "File type: ${COMPLETION-CANDIDATES}", paramLabel = "<type>")
    private FileType type;
    @CommandLine.ArgGroup(exclusive = false, heading = "Delimited and fixed-width file options%n")
    private FileImportOptions options = new FileImportOptions();
    @CommandLine.ArgGroup(exclusive = false, heading = "Processor options%n")
    private ProcessorOptions processorOptions = new ProcessorOptions();

    @Override
    protected Flow flow(StepBuilderFactory stepBuilderFactory) throws Exception {
        Assert.isTrue(!ObjectUtils.isEmpty(files), "No file specified");
        List<String> expandedFiles = FileUtils.expand(files);
        if (ObjectUtils.isEmpty(expandedFiles)) {
            throw new FileNotFoundException("File not found: " + String.join(", ", files));
        }
        List<Step> steps = new ArrayList<>();
        for (String file : expandedFiles) {
            FileType fileType = type(file);
            if (fileType == null) {
                throw new IllegalArgumentException("Could not determine type of file " + file);
            }
            Resource resource = options.inputResource(file);
            AbstractItemStreamItemReader<Map<String, Object>> reader = reader(file, fileType, resource);
            reader.setName(file + "-reader");
            StepBuilder stepBuilder = stepBuilderFactory.get(file + "-file-import-step");
            FaultTolerantStepBuilder<Map<String, Object>, Map<String, Object>> step = step(stepBuilder, "Importing " + file, reader);
            step.skip(FlatFileParseException.class);
            steps.add(step.build());
        }
        return flow(steps.toArray(new Step[0]));
    }

    private FileType type(String file) {
        if (type == null) {
            String extension = FileUtils.extension(file);
            if (extension != null) {
                switch (extension.toLowerCase()) {
                    case FileUtils.EXTENSION_CSV:
                    case FileUtils.EXTENSION_PSV:
                    case FileUtils.EXTENSION_TSV:
                        return FileType.DELIMITED;
                    case FileUtils.EXTENSION_FW:
                        return FileType.FIXED;
                    case FileUtils.EXTENSION_JSON:
                        return FileType.JSON;
                    case FileUtils.EXTENSION_XML:
                        return FileType.XML;
                }
            }
            return null;
        }
        return type;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private AbstractItemStreamItemReader<Map<String, Object>> reader(String file, FileType fileType, Resource resource) {
        switch (fileType) {
            case DELIMITED:
                DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
                tokenizer.setDelimiter(delimiter(file));
                tokenizer.setQuoteCharacter(options.getQuoteCharacter());
                if (!ObjectUtils.isEmpty(options.getIncludedFields())) {
                    tokenizer.setIncludedFields(options.getIncludedFields());
                }
                log.debug("Creating delimited reader with {} for file {}", options, file);
                return flatFileReader(resource, tokenizer);
            case FIXED:
                FixedLengthTokenizer fixedLengthTokenizer = new FixedLengthTokenizer();
                RangeArrayPropertyEditor editor = new RangeArrayPropertyEditor();
                Assert.notEmpty(options.getColumnRanges(), "Column ranges are required");
                editor.setAsText(String.join(",", options.getColumnRanges()));
                Range[] ranges = (Range[]) editor.getValue();
                if (ranges.length == 0) {
                    throw new IllegalArgumentException("Invalid ranges specified: " + Arrays.toString(options.getColumnRanges()));
                }
                fixedLengthTokenizer.setColumns(ranges);
                log.debug("Creating fixed-width reader with {} for file {}", options, file);
                return flatFileReader(resource, fixedLengthTokenizer);
            case XML:
                log.debug("Creating XML reader for file {}", file);
                return (XmlItemReader) FileUtils.xmlReader(resource, Map.class);
            default:
                log.debug("Creating JSON reader for file {}", file);
                return (JsonItemReader) FileUtils.jsonReader(resource, Map.class);
        }
    }

    private String delimiter(String file) {
        if (options.getDelimiter() == null) {
            String extension = FileUtils.extension(file);
            if (extension != null) {
                switch (extension.toLowerCase()) {
                    case FileUtils.EXTENSION_CSV:
                        return DelimitedLineTokenizer.DELIMITER_COMMA;
                    case FileUtils.EXTENSION_PSV:
                        return DELIMITER_PIPE;
                    case FileUtils.EXTENSION_TSV:
                        return DelimitedLineTokenizer.DELIMITER_TAB;
                }
            }
            throw new IllegalArgumentException("Could not determine delimiter for extension " + extension);
        }
        return options.getDelimiter();
    }

    @Override
    protected ItemProcessor<Map<String, Object>, Map<String, Object>> processor() throws NoSuchMethodException {
        return processorOptions.processor(getRedisOptions());
    }

    private FlatFileItemReader<Map<String, Object>> flatFileReader(Resource resource, AbstractLineTokenizer tokenizer) {
        if (!ObjectUtils.isEmpty(options.getNames())) {
            tokenizer.setNames(options.getNames());
        }
        FlatFileItemReaderBuilder<Map<String, Object>> builder = new FlatFileItemReaderBuilder<>();
        builder.resource(resource);
        builder.encoding(options.getEncoding().name());
        builder.lineTokenizer(tokenizer);
        builder.recordSeparatorPolicy(recordSeparatorPolicy());
        builder.linesToSkip(options.linesToSkip());
        builder.strict(true);
        builder.saveState(false);
        builder.fieldSetMapper(new MapFieldSetMapper());
        builder.skippedLinesCallback(new HeaderCallbackHandler(tokenizer));
        return builder.build();
    }

    private RecordSeparatorPolicy recordSeparatorPolicy() {
        return new DefaultRecordSeparatorPolicy(options.getQuoteCharacter().toString(), options.getContinuationString());
    }

}
