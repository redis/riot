package com.redislabs.riot.file;

import com.redislabs.riot.AbstractExportCommand;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.FieldExtractor;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.batch.item.resource.FlatResourceItemWriterBuilder;
import org.springframework.batch.item.resource.JsonResourceItemWriterBuilder;
import org.springframework.batch.item.support.PassThroughItemProcessor;
import org.springframework.batch.item.xml.builder.StaxEventItemWriterBuilder;
import org.springframework.core.io.Resource;
import org.springframework.core.io.WritableResource;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;
import picocli.CommandLine;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

@SuppressWarnings({"rawtypes", "unchecked"})
@CommandLine.Command(name = "export", aliases = "e", description = "Export to file")
public class FileExportCommand extends AbstractExportCommand<Object> {

    @CommandLine.Mixin
    private FileOptions fileOptions = new FileOptions();
    @CommandLine.Option(names = "--append", description = "Append to file if it exists")
    private boolean append;
    @CommandLine.Option(names = "--force-sync", description = "Force-sync changes to disk on flush", hidden = true)
    private boolean forceSync;
    @CommandLine.Option(names = "--line-sep", description = "String to separate lines (default: system default)", paramLabel = "<string>")
    private String lineSeparator = FlatFileItemWriter.DEFAULT_LINE_SEPARATOR;
    @CommandLine.Option(names = "--line-format", description = "Format for line aggregation", paramLabel = "<string>")
    private String lineFormat;
    @CommandLine.Option(names = "--locale", description = "Locale", paramLabel = "<tag>")
    private Locale locale = Locale.ENGLISH;
    @CommandLine.Option(names = "--max-length", description = "Max length of the formatted string", paramLabel = "<int>")
    private Integer maxLength;
    @CommandLine.Option(names = "--min-length", description = "Min length of the formatted string", paramLabel = "<int>")
    private Integer minLength;
    @CommandLine.Option(names = "--root", description = "XML root element tag name", paramLabel = "<string>")
    private String root;

    @Override
    protected ItemProcessor processor() {
        ResourceHelper helper = new ResourceHelper(fileOptions);
        switch (helper.getFileType()) {
            case DELIMITED:
            case FIXED:
                return mapProcessor();
            case JSON:
            case XML:
                return targetObjectProcessor();
        }
        throw new IllegalArgumentException("Unknown file type");
    }

    @Override
    protected ItemWriter writer() throws IOException {
        ResourceHelper helper = new ResourceHelper(fileOptions);
        WritableResource resource = helper.getOutputResource();
        FileType fileType = helper.getFileType();
        switch (fileType) {
            case DELIMITED:
                FlatResourceItemWriterBuilder<Map<String, String>> delimitedWriterBuilder = flatWriterBuilder("delimited-resource-item-writer", resource);
                if (fileOptions.isHeader()) {
                    delimitedWriterBuilder.headerCallback(w -> w.write(String.join(helper.getDelimiter(), fileOptions.getNames())));
                }
                FlatResourceItemWriterBuilder.DelimitedBuilder<Map<String, String>> delimited = delimitedWriterBuilder.delimited();
                delimited.delimiter(helper.getDelimiter());
                delimited.fieldExtractor(new MapFieldExtractor());
                if (fileOptions.getNames().length > 0) {
                    delimited.names(fileOptions.getNames());
                }
                return delimitedWriterBuilder.build();
            case FIXED:
                FlatResourceItemWriterBuilder<Map<String, String>> fixedWriterBuilder = flatWriterBuilder("formatted-resource-item-writer", resource);
                if (fileOptions.isHeader()) {
                    fixedWriterBuilder.headerCallback(w -> w.write(String.format(locale, lineFormat, (Object[]) fileOptions.getNames())));
                }
                FlatResourceItemWriterBuilder.FormattedBuilder<Map<String, String>> formatted = fixedWriterBuilder.formatted();
                formatted.fieldExtractor(new MapFieldExtractor());
                if (fileOptions.getNames().length > 0) {
                    formatted.names(fileOptions.getNames());
                }
                formatted.format(lineFormat);
                formatted.locale(locale);
                if (minLength != null) {
                    formatted.minimumLength(minLength);
                }
                if (maxLength != null) {
                    formatted.maximumLength(maxLength);
                }
                return fixedWriterBuilder.build();
            case JSON:
                JsonResourceItemWriterBuilder jsonWriterBuilder = new JsonResourceItemWriterBuilder();
                jsonWriterBuilder.name("json-resource-item-writer");
                jsonWriterBuilder.append(append);
                jsonWriterBuilder.encoding(fileOptions.getEncoding());
                jsonWriterBuilder.jsonObjectMarshaller(new JacksonJsonObjectMarshaller<>());
                jsonWriterBuilder.lineSeparator(lineSeparator);
                jsonWriterBuilder.resource(resource);
                jsonWriterBuilder.saveState(false);
                return jsonWriterBuilder.build();
            case XML:
                StaxEventItemWriterBuilder xmlWriterBuilder = new StaxEventItemWriterBuilder<>();
                xmlWriterBuilder.name("xml-resource-item-writer");
                xmlWriterBuilder.encoding(fileOptions.getEncoding());
                xmlWriterBuilder.forceSync(forceSync);
                Jaxb2Marshaller marshaller = new Jaxb2Marshaller();
                marshaller.setClassesToBeBound(targetClass());
                xmlWriterBuilder.marshaller(marshaller);
                xmlWriterBuilder.rootTagName(root);
                xmlWriterBuilder.resource(resource);
                xmlWriterBuilder.saveState(false);
                return xmlWriterBuilder.build();
        }
        throw new IllegalArgumentException("Unknown file type");
    }

    private FlatResourceItemWriterBuilder<Map<String, String>> flatWriterBuilder(String name, Resource resource) {
        FlatResourceItemWriterBuilder<Map<String, String>> builder = new FlatResourceItemWriterBuilder<>();
        builder.name(name);
        builder.append(append);
        builder.encoding(fileOptions.getEncoding());
        builder.lineSeparator(lineSeparator);
        builder.resource(resource);
        builder.saveState(false);
        return builder;
    }

    private class MapFieldExtractor implements FieldExtractor<Map<String, String>> {

        @Override
        public String[] extract(Map<String, String> item) {
            String[] names = fileOptions.getNames();
            String[] fields = new String[names.length];
            for (int index = 0; index < names.length; index++) {
                fields[index] = item.get(names[index]);
            }
            return fields;
        }

    }

}
