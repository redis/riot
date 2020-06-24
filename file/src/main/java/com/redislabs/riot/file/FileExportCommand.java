package com.redislabs.riot.file;

import com.redislabs.riot.AbstractExportCommand;
import com.redislabs.riot.processor.KeyValueItemProcessor;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.file.FlatFileHeaderCallback;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.batch.item.redis.support.KeyValue;
import org.springframework.batch.item.resource.*;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.batch.item.support.PassThroughItemProcessor;
import org.springframework.batch.item.xml.StaxEventItemWriter;
import org.springframework.batch.item.xml.builder.StaxEventItemWriterBuilder;
import org.springframework.core.io.Resource;
import org.springframework.core.io.WritableResource;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;
import org.springframework.util.Assert;
import picocli.CommandLine;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

@CommandLine.Command(name = "export", description = "Export to file")
public class FileExportCommand extends AbstractExportCommand<Object> {

    @CommandLine.Mixin
    private FileOptions options = new FileOptions();
    @CommandLine.Option(names = "--append", description = "Append to file if it exists")
    private boolean append;
    @CommandLine.Option(names = "--force-sync", description = "Force-sync changes to disk on flush", hidden = true)
    private boolean forceSync;
    @CommandLine.Option(names = "--line-sep", description = "String to separate lines (default: system default)", paramLabel = "<string>")
    private String lineSeparator = FlatFileItemWriter.DEFAULT_LINE_SEPARATOR;
    @CommandLine.Option(names = "--format", description = "Format string used to aggregate items", paramLabel = "<string>")
    private String format;
    @CommandLine.Option(names = "--locale", description = "Locale", paramLabel = "<tag>")
    private Locale locale = Locale.ENGLISH;
    @CommandLine.Option(names = "--max-length", description = "Max length of the formatted string", paramLabel = "<int>")
    private Integer maxLength;
    @CommandLine.Option(names = "--min-length", description = "Min length of the formatted string", paramLabel = "<int>")
    private Integer minLength;
    @CommandLine.Option(names = "--root", description = "XML root element tag name", paramLabel = "<string>")
    private String root;

    @Override
    protected ItemProcessor<KeyValue<String>, Object> processor() throws Exception {
        switch (options.getFileType()) {
            case DELIMITED:
            case FIXED:
                return (ItemProcessor) KeyValueItemProcessor.builder().build();
            case JSON:
            case XML:
                return (ItemProcessor) new PassThroughItemProcessor<KeyValue<String>>();
        }
        throw new IllegalArgumentException("Unknown file type");
    }

    private WritableResource resource() throws IOException {
        if (options.isConsole()) {
            return new StandardOutputResource();
        }
        Resource resource = options.getResource();
        Assert.isInstanceOf(WritableResource.class, resource);
        WritableResource writable = (WritableResource) resource;
        if (options.isGzip()) {
            return new GZIPOutputStreamResource(writable.getOutputStream(), writable.getDescription());
        }
        return writable;
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    protected AbstractItemStreamItemWriter<Object> writer() throws IOException {
        switch (options.getFileType()) {
            case DELIMITED:
                return (FlatResourceItemWriter) delimitedWriter();
            case FIXED:
                return (FlatResourceItemWriter) formattedWriter();
            case JSON:
                return (JsonResourceItemWriter) jsonWriter();
            case XML:
                return (StaxEventItemWriter) xmlWriter();
        }
        throw new IllegalArgumentException("Unknown file type");
    }

    private FlatResourceItemWriterBuilder<Map<String, String>> flatWriterBuilder() throws IOException {
        FlatResourceItemWriterBuilder<Map<String, String>> builder = new FlatResourceItemWriterBuilder<>();
        builder.append(append);
        builder.encoding(options.getEncoding());
        builder.lineSeparator(lineSeparator);
        builder.resource(resource());
        builder.saveState(false);
        builder.headerCallback(headerCallback());
        return builder;
    }

    private FlatFileHeaderCallback headerCallback() {
        if (options.isHeader()) {
            return new HeaderCallback(header());
        }
        return null;
    }

    private String header() {
        switch (options.getFileType()) {
            case DELIMITED:
                return String.join(options.getDelimiter(), options.getNames());
            case FIXED:
                return String.format(locale, format, (Object[]) options.getNames());
        }
        throw new IllegalArgumentException("Unknown file type");
    }

    private JsonResourceItemWriter<KeyValue<String>> jsonWriter() throws IOException {
        JsonResourceItemWriterBuilder<KeyValue<String>> builder = new JsonResourceItemWriterBuilder<>();
        builder.name("json-resource-item-writer");
        builder.append(append);
        builder.encoding(options.getEncoding());
        builder.jsonObjectMarshaller(new JacksonJsonObjectMarshaller<>());
        builder.lineSeparator(lineSeparator);
        builder.resource(resource());
        builder.saveState(false);
        return builder.build();
    }

    private StaxEventItemWriter<KeyValue<String>> xmlWriter() throws IOException {
        StaxEventItemWriterBuilder<KeyValue<String>> builder = new StaxEventItemWriterBuilder<>();
        builder.name("xml-resource-item-writer");
        builder.encoding(options.getEncoding());
        builder.forceSync(forceSync);
        Jaxb2Marshaller marshaller = new Jaxb2Marshaller();
        marshaller.setClassesToBeBound(KeyValue.class);
        builder.marshaller(marshaller);
        builder.rootTagName(root);
        builder.resource(resource());
        builder.saveState(false);
        return builder.build();
    }

    private FlatResourceItemWriter<Map<String, String>> delimitedWriter() throws IOException {
        FlatResourceItemWriterBuilder<Map<String, String>> builder = flatWriterBuilder();
        builder.name("delimited-resource-item-writer");
        FlatResourceItemWriterBuilder.DelimitedBuilder<Map<String, String>> delimited = builder.delimited();
        delimited.delimiter(options.getDelimiter());
        delimited.fieldExtractor(MapFieldExtractor.builder().names(options.getNames()).build());
        if (options.getNames().length > 0) {
            delimited.names(options.getNames());
        }
        return builder.build();
    }

    private FlatResourceItemWriter<Map<String, String>> formattedWriter() throws IOException {
        FlatResourceItemWriterBuilder<Map<String, String>> builder = flatWriterBuilder();
        FlatResourceItemWriterBuilder.FormattedBuilder<Map<String, String>> formatted = builder.formatted();
        builder.name("formatted-resource-item-writer");
        formatted.fieldExtractor(MapFieldExtractor.builder().names(options.getNames()).build());
        if (options.getNames().length > 0) {
            formatted.names(options.getNames());
        }
        formatted.format(format);
        formatted.locale(locale);
        if (minLength != null) {
            formatted.minimumLength(minLength);
        }
        if (maxLength != null) {
            formatted.maximumLength(maxLength);
        }
        return builder.build();
    }
}
