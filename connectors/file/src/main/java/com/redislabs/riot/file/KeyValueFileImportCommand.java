package com.redislabs.riot.file;

import com.redislabs.riot.KeyValueProcessingOptions;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineCallbackHandler;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.separator.DefaultRecordSeparatorPolicy;
import org.springframework.batch.item.file.transform.AbstractLineTokenizer;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.FixedLengthTokenizer;
import org.springframework.batch.item.file.transform.Range;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;
import org.springframework.batch.item.xml.XmlItemReader;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;
import picocli.CommandLine;
import picocli.CommandLine.Command;

import java.util.Map;

@Slf4j
@Setter
@Command(name = "import", description = "Import file(s) into Redis")
public class KeyValueFileImportCommand extends AbstractFileImportCommand<Map<String, Object>> {

    @CommandLine.Mixin
    protected FileImportOptions options = new FileImportOptions();
    @CommandLine.Mixin
    private KeyValueProcessingOptions processingOptions = new KeyValueProcessingOptions();

    protected AbstractItemStreamItemReader<Map<String, Object>> reader(String file, FileType fileType, Resource resource) {
        switch (fileType) {
            case DELIMITED:
                DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
                tokenizer.setDelimiter(options.delimiter(file));
                tokenizer.setQuoteCharacter(options.getQuoteCharacter());
                if (!options.getIncludedFields().isEmpty()) {
                    tokenizer.setIncludedFields(options.getIncludedFields().stream().mapToInt(i -> i).toArray());
                }
                return flatFileReader(resource, tokenizer);
            case FIXED:
                FixedLengthTokenizer fixedLengthTokenizer = new FixedLengthTokenizer();
                Assert.notEmpty(options.getColumnRanges(), "Column ranges are required");
                fixedLengthTokenizer.setColumns(options.getColumnRanges().toArray(new Range[0]));
                return flatFileReader(resource, fixedLengthTokenizer);
            case JSON:
                return (JsonItemReader) FileUtils.jsonReader(resource, Map.class);
            case XML:
                return (XmlItemReader) FileUtils.xmlReader(resource, Map.class);
        }
        throw new IllegalArgumentException("Unsupported file type: " + fileType);
    }

    @Override
    protected ItemProcessor<Map<String, Object>, Map<String, Object>> processor() {
        return processingOptions.processor(connection());
    }

    private FlatFileItemReader<Map<String, Object>> flatFileReader(Resource resource, AbstractLineTokenizer tokenizer) {
        tokenizer.setNames(options.getNames().toArray(new String[0]));
        return new FlatFileItemReaderBuilder<Map<String, Object>>().resource(resource).encoding(getFileOptions().getEncoding()).lineTokenizer(tokenizer).linesToSkip(options.linesToSkip()).strict(true).saveState(false).fieldSetMapper(new MapFieldSetMapper()).recordSeparatorPolicy(new DefaultRecordSeparatorPolicy()).skippedLinesCallback(new HeaderCallbackHandler(tokenizer)).build();
    }

    private static class HeaderCallbackHandler implements LineCallbackHandler {

        private final AbstractLineTokenizer tokenizer;

        public HeaderCallbackHandler(AbstractLineTokenizer tokenizer) {
            this.tokenizer = tokenizer;
        }

        @Override
        public void handleLine(String line) {
            log.debug("Found header {}", line);
            tokenizer.setNames(tokenizer.tokenize(line).getValues());
        }
    }

}
