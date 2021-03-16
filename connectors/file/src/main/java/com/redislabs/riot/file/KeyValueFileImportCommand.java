package com.redislabs.riot.file;

import com.redislabs.riot.AbstractImportCommand;
import com.redislabs.riot.KeyValueProcessingOptions;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineCallbackHandler;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.separator.DefaultRecordSeparatorPolicy;
import org.springframework.batch.item.file.separator.RecordSeparatorPolicy;
import org.springframework.batch.item.file.transform.AbstractLineTokenizer;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.FixedLengthTokenizer;
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
import java.util.List;
import java.util.Map;

@Slf4j
@Command(name = "import", description = "Import files (delimited, fixed-width, JSON, XML) into Redis.")
public class KeyValueFileImportCommand extends AbstractImportCommand<Map<String, Object>, Map<String, Object>> {

    private static final String DELIMITER_PIPE = "|";

    enum FileType {
        CSV, PSV, TSV, FW, JSON, XML
    }

    @SuppressWarnings("unused")
    @CommandLine.Parameters(arity = "1..*", description = "One ore more files or URLs", paramLabel = "FILE")
    private String[] files;
    @CommandLine.Option(names = {"-t", "--type"}, description = "File type: ${COMPLETION-CANDIDATES}", paramLabel = "<type>")
    private FileType type;
    @CommandLine.Mixin
    private FileOptions fileOptions = FileOptions.builder().build();
    @CommandLine.Mixin
    private FlatFileImportOptions flatFileOptions = FlatFileImportOptions.builder().build();
    @CommandLine.Mixin
    private KeyValueProcessingOptions processingOptions = KeyValueProcessingOptions.builder().build();

    @Override
    protected Flow flow() throws Exception {
        List<String> expandedFiles = FileUtils.expand(files);
        if (ObjectUtils.isEmpty(expandedFiles)) {
            throw new FileNotFoundException("File not found: " + String.join(", ", files));
        }
        List<Step> steps = new ArrayList<>();
        for (String file : expandedFiles) {
            FileType fileType = FileUtils.type(FileType.class, type, file);
            Resource resource = FileUtils.inputResource(file, fileOptions);
            AbstractItemStreamItemReader<Map<String, Object>> reader = reader(file, fileType, resource);
            String name = FileUtils.filename(resource);
            reader.setName(name);
            steps.add(step(name + "-file-import-step", "Importing " + name, reader).build());
        }
        return flow(steps.toArray(new Step[0]));
    }

    private AbstractItemStreamItemReader<Map<String, Object>> reader(String file, FileType fileType, Resource resource) {
        switch (fileType) {
            case CSV:
            case PSV:
            case TSV:
                DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
                tokenizer.setDelimiter(delimiter(fileType));
                tokenizer.setQuoteCharacter(flatFileOptions.getQuoteCharacter());
                if (!ObjectUtils.isEmpty(flatFileOptions.getIncludedFields())) {
                    tokenizer.setIncludedFields(flatFileOptions.getIncludedFields());
                }
                log.info("Creating delimited reader with {} for file {}", flatFileOptions, file);
                return flatFileReader(resource, tokenizer);
            case FW:
                FixedLengthTokenizer fixedLengthTokenizer = new FixedLengthTokenizer();
                Assert.notEmpty(flatFileOptions.getColumnRanges(), "Column ranges are required");
                fixedLengthTokenizer.setColumns(flatFileOptions.getColumnRanges());
                log.info("Creating fixed-width reader with {} for file {}", flatFileOptions, file);
                return flatFileReader(resource, fixedLengthTokenizer);
            case JSON:
                log.info("Creating JSON reader for file {}", file);
                return (JsonItemReader) FileUtils.jsonReader(resource, Map.class);
            case XML:
                log.info("Creating XML reader for file {}", file);
                return (XmlItemReader) FileUtils.xmlReader(resource, Map.class);
        }
        throw new IllegalArgumentException("Unsupported file type: " + fileType);
    }

    private String delimiter(FileType fileType) {
        if (flatFileOptions.getDelimiter() != null) {
            return flatFileOptions.getDelimiter();
        }
        switch (fileType) {
            case TSV:
                return DelimitedLineTokenizer.DELIMITER_TAB;
            case PSV:
                return DELIMITER_PIPE;
            default:
                return DelimitedLineTokenizer.DELIMITER_COMMA;
        }
    }

    @Override
    protected ItemProcessor<Map<String, Object>, Map<String, Object>> processor() {
        return processingOptions.processor(connection);
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

    private static class HeaderCallbackHandler implements LineCallbackHandler {

        private final AbstractLineTokenizer tokenizer;

        public HeaderCallbackHandler(AbstractLineTokenizer tokenizer) {
            this.tokenizer = tokenizer;
        }

        @Override
        public void handleLine(String line) {
            log.info("Found header {}", line);
            tokenizer.setNames(tokenizer.tokenize(line).getValues());
        }
    }

}
