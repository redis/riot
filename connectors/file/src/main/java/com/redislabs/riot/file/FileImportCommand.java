package com.redislabs.riot.file;

import com.redislabs.riot.AbstractImportCommand;
import com.redislabs.riot.ProcessorOptions;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.separator.DefaultRecordSeparatorPolicy;
import org.springframework.batch.item.file.separator.RecordSeparatorPolicy;
import org.springframework.batch.item.file.transform.*;
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
@Command(name = "import", description = "Import delimited, fixed-width, JSON, or XML files into Redis.")
public class FileImportCommand extends AbstractImportCommand<Map<String, Object>, Map<String, Object>> {

    private enum FileType {
        DELIMITED, FIXED, JSON, XML
    }

    private static final String DELIMITER_PIPE = "|";

    @SuppressWarnings("unused")
    @CommandLine.Parameters(arity = "0..*", description = "One ore more files or URLs", paramLabel = "FILE")
    private String[] files;
    @CommandLine.Option(names = {"-t", "--type"}, description = "File type: ${COMPLETION-CANDIDATES}", paramLabel = "<type>")
    private FileType type;
    @CommandLine.Mixin
    private FileOptions fileOptions = FileOptions.builder().build();
    @CommandLine.Mixin
    private FlatFileImportOptions flatFileOptions = FlatFileImportOptions.builder().build();
    @CommandLine.Mixin
    private ProcessorOptions processingOptions = ProcessorOptions.builder().build();

    @Override
    protected Flow flow() throws Exception {
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
            Resource resource = FileUtils.inputResource(file, fileOptions);
            AbstractItemStreamItemReader<Map<String, Object>> reader = reader(file, fileType, resource);
            String name = FileUtils.filename(resource);
            reader.setName(name);
            steps.add(step(name + "-file-import-step", "Importing " + name, reader).build());
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
                tokenizer.setQuoteCharacter(flatFileOptions.getQuoteCharacter());
                if (!ObjectUtils.isEmpty(flatFileOptions.getIncludedFields())) {
                    tokenizer.setIncludedFields(flatFileOptions.getIncludedFields());
                }
                log.info("Creating delimited reader with {} for file {}", flatFileOptions, file);
                return flatFileReader(resource, tokenizer);
            case FIXED:
                FixedLengthTokenizer fixedLengthTokenizer = new FixedLengthTokenizer();
                RangeArrayPropertyEditor editor = new RangeArrayPropertyEditor();
                Assert.notEmpty(flatFileOptions.getColumnRanges(), "Column ranges are required");
                editor.setAsText(String.join(",", flatFileOptions.getColumnRanges()));
                Range[] ranges = (Range[]) editor.getValue();
                if (ranges.length == 0) {
                    throw new IllegalArgumentException("Invalid ranges specified: " + Arrays.toString(flatFileOptions.getColumnRanges()));
                }
                fixedLengthTokenizer.setColumns(ranges);
                log.info("Creating fixed-width reader with {} for file {}", flatFileOptions, file);
                return flatFileReader(resource, fixedLengthTokenizer);
            case XML:
                log.info("Creating XML reader for file {}", file);
                return (XmlItemReader) FileUtils.xmlReader(resource, Map.class);
            default:
                log.info("Creating JSON reader for file {}", file);
                return (JsonItemReader) FileUtils.jsonReader(resource, Map.class);
        }
    }

    private String delimiter(String file) {
        if (flatFileOptions.getDelimiter() == null) {
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
        return flatFileOptions.getDelimiter();
    }

    @Override
    protected ItemProcessor<Map<String, Object>, Map<String, Object>> processor() throws NoSuchMethodException {
        return processingOptions.processor(client);
    }

    private FlatFileItemReader<Map<String, Object>> flatFileReader(Resource resource, AbstractLineTokenizer tokenizer) {
        if (!ObjectUtils.isEmpty(flatFileOptions.getNames())) {
            tokenizer.setNames(flatFileOptions.getNames());
        }
        FlatFileItemReaderBuilder<Map<String, Object>> builder = new FlatFileItemReaderBuilder<>();
        builder.resource(resource);
        builder.encoding(fileOptions.getEncoding());
        builder.lineTokenizer(tokenizer);
        builder.recordSeparatorPolicy(recordSeparatorPolicy());
        builder.linesToSkip(flatFileOptions.linesToSkip());
        builder.strict(true);
        builder.saveState(false);
        builder.fieldSetMapper(new MapFieldSetMapper());
        builder.skippedLinesCallback(new HeaderCallbackHandler(tokenizer));
        return builder.build();
    }

    private RecordSeparatorPolicy recordSeparatorPolicy() {
        return new DefaultRecordSeparatorPolicy(flatFileOptions.getQuoteCharacter().toString(), flatFileOptions.getContinuationString());
    }

}
