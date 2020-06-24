package com.redislabs.riot.file;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redislabs.riot.AbstractImportCommand;
import com.redislabs.riot.processor.MapFlattener;
import com.redislabs.riot.processor.MapProcessor;
import com.redislabs.riot.processor.ObjectMapToStringMapProcessor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.file.DefaultBufferedReaderFactory;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.separator.DefaultRecordSeparatorPolicy;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.Range;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.builder.JsonItemReaderBuilder;
import org.springframework.batch.item.resource.StandardInputResource;
import org.springframework.batch.item.xml.XmlObjectReader;
import org.springframework.batch.item.xml.builder.XmlItemReaderBuilder;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;
import picocli.CommandLine;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.*;

@Slf4j
@CommandLine.Command(name = "import", description = "Import file")
public class FileImportCommand extends AbstractImportCommand<Map<String, String>> {

    @CommandLine.Mixin
    private final FileOptions options = new FileOptions();
    @CommandLine.Option(names = {"--skip"}, description = "Lines to skip at start of file (default: ${DEFAULT-VALUE})", paramLabel = "<count>")
    private int linesToSkip = 0;
    @CommandLine.Option(names = "--include", arity = "1..*", description = "Field indices to include (0-based)", paramLabel = "<index>")
    private int[] includedFields = new int[0];
    @CommandLine.Option(names = "--ranges", arity = "1..*", description = "Fixed-width column ranges", paramLabel = "<int>")
    private Range[] columnRanges = new Range[0];
    @CommandLine.Option(names = {"-q", "--quote"}, description = "Escape character (default: ${DEFAULT-VALUE})", paramLabel = "<char>")
    private Character quoteCharacter = DelimitedLineTokenizer.DEFAULT_QUOTE_CHARACTER;
    @CommandLine.Option(arity = "1..*", names = "--regex", description = "Extract named values from source field using regex", paramLabel = "<field=exp>")
    private Map<String, String> regexes = new HashMap<>();

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    protected ItemReader<Map<String, String>> reader() throws Exception {
        FileType fileType = options.getFileType();
        Resource resource = resource();
        switch (fileType) {
            case DELIMITED:
                FlatFileItemReaderBuilder delimitedReaderBuilder = flatFileReaderBuilder(resource);
                FlatFileItemReaderBuilder.DelimitedBuilder delimitedBuilder = delimitedReaderBuilder.delimited();
                delimitedBuilder.delimiter(options.getDelimiter());
                delimitedBuilder.includedFields(includedFields());
                delimitedBuilder.quoteCharacter(quoteCharacter);
                String[] fieldNames = options.getNames();
                if (options.isHeader()) {
                    BufferedReader reader = new DefaultBufferedReaderFactory().create(resource, options.getEncoding());
                    DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
                    tokenizer.setDelimiter(options.getDelimiter());
                    tokenizer.setQuoteCharacter(quoteCharacter);
                    if (includedFields.length > 0) {
                        tokenizer.setIncludedFields(includedFields);
                    }
                    fieldNames = tokenizer.tokenize(reader.readLine()).getValues();
                    log.debug("Found header {}", Arrays.asList(fieldNames));
                }
                if (fieldNames == null || fieldNames.length == 0) {
                    throw new IOException("No fields specified");
                }
                delimitedBuilder.names(fieldNames);
                return delimitedReaderBuilder.build();
            case FIXED:
                FlatFileItemReaderBuilder fixedReaderBuilder = flatFileReaderBuilder(resource);
                FlatFileItemReaderBuilder.FixedLengthBuilder fixedLength = fixedReaderBuilder.fixedLength();
                Assert.notEmpty(columnRanges, "Column ranges are required");
                fixedLength.columns(columnRanges);
                fixedLength.names(options.getNames());
                return fixedReaderBuilder.build();
            case JSON:
                JsonItemReaderBuilder<Map> jsonReaderBuilder = new JsonItemReaderBuilder<>();
                jsonReaderBuilder.name("json-file-reader");
                jsonReaderBuilder.resource(resource());
                JacksonJsonObjectReader<Map> jsonObjectReader = new JacksonJsonObjectReader<>(Map.class);
                jsonObjectReader.setMapper(new ObjectMapper());
                jsonReaderBuilder.jsonObjectReader(jsonObjectReader);
                return (ItemReader) jsonReaderBuilder.build();
            case XML:
                XmlItemReaderBuilder<Map> xmlReaderBuilder = new XmlItemReaderBuilder<>();
                xmlReaderBuilder.name("xml-file-reader");
                xmlReaderBuilder.resource(resource());
                XmlObjectReader<Map> xmlObjectReader = new XmlObjectReader<>(Map.class);
                xmlObjectReader.setMapper(new XmlMapper());
                xmlReaderBuilder.xmlObjectReader(xmlObjectReader);
                return (ItemReader) xmlReaderBuilder.build();
        }
        throw new IllegalArgumentException("Unknown file type: " + fileType);
    }

    private Integer[] includedFields() {
        Integer[] fields = new Integer[includedFields.length];
        for (int index = 0; index < includedFields.length; index++) {
            fields[index] = includedFields[index];
        }
        return fields;
    }

    private FlatFileItemReaderBuilder<Map<String, String>> flatFileReaderBuilder(Resource resource) {
        FlatFileItemReaderBuilder<Map<String, String>> flatFileReaderBuilder = new FlatFileItemReaderBuilder<>();
        flatFileReaderBuilder.name("flat-file-reader");
        flatFileReaderBuilder.resource(resource);
        flatFileReaderBuilder.encoding(options.getEncoding());
        flatFileReaderBuilder.linesToSkip(linesToSkip);
        flatFileReaderBuilder.strict(true);
        flatFileReaderBuilder.saveState(false);
        flatFileReaderBuilder.fieldSetMapper(new MapFieldSetMapper());
        flatFileReaderBuilder.recordSeparatorPolicy(new DefaultRecordSeparatorPolicy());
        if (options.isHeader() && linesToSkip == 0) {
            flatFileReaderBuilder.linesToSkip(1);
        }
        return flatFileReaderBuilder;
    }

    private Resource resource() throws IOException {
        if (options.isConsole()) {
            return new StandardInputResource();
        }
        Resource resource = options.getResource();
        if (options.isGzip()) {
            return new GZIPInputStreamResource(resource.getInputStream(), resource.getDescription());
        }
        return resource;

    }

    @Override
    @SuppressWarnings("unchecked")
    protected ItemProcessor<Map<String, String>, Object> processor() {
        FileType fileType = options.getFileType();
        switch (fileType) {
            case DELIMITED:
            case FIXED:
                return stringMapProcessor();
            case JSON:
            case XML:
                return processor(Collections.singletonList(MapFlattener.builder().build()));
        }
        throw new IllegalArgumentException("Unknown file type: " + fileType);
    }


    @SuppressWarnings("rawtypes")
    private ItemProcessor stringMapProcessor() {
        List<ItemProcessor> processors = new ArrayList<>();
        if (!regexes.isEmpty()) {
            processors.add(MapProcessor.builder().regexes(regexes).build());
        }
        if (!getSpel().isEmpty()) {
            processors.add(spelProcessor());
            processors.add(ObjectMapToStringMapProcessor.builder().build());
        }
        return processor(processors);
    }
}
