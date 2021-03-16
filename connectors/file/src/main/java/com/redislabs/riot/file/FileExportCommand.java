package com.redislabs.riot.file;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redislabs.riot.AbstractExportCommand;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.batch.item.json.JsonObjectMarshaller;
import org.springframework.batch.item.redis.support.DataStructure;
import org.springframework.batch.item.resource.support.JsonResourceItemWriterBuilder;
import org.springframework.batch.item.xml.support.XmlResourceItemWriterBuilder;
import org.springframework.core.io.WritableResource;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

import java.io.IOException;

@Slf4j
@Command(name = "export", description = "Export Redis data to JSON or XML files")
public class FileExportCommand extends AbstractExportCommand<DataStructure<String>> {

    @SuppressWarnings("unused")
    @CommandLine.Parameters(arity = "1", description = "File path or URL", paramLabel = "FILE")
    private String file;
    @CommandLine.Option(names = {"-t", "--type"}, description = "File type: ${COMPLETION-CANDIDATES}", paramLabel = "<type>")
    private DumpFileType type;
    @Mixin
    private FileExportOptions exportOptions = FileExportOptions.builder().build();
    @Mixin
    private FileOptions fileOptions = FileOptions.builder().build();

    @Override
    protected Flow flow() throws Exception {
        return flow(step(null, writer()).build());
    }

    private ItemWriter<DataStructure<String>> writer() throws IOException {
        WritableResource resource = FileUtils.outputResource(file, fileOptions);
        DumpFileType fileType = type == null ? DumpFileType.of(file) : type;
        switch (fileType) {
            case XML:
                XmlResourceItemWriterBuilder<DataStructure<String>> xmlWriterBuilder = new XmlResourceItemWriterBuilder<>();
                xmlWriterBuilder.name("xml-resource-item-writer");
                xmlWriterBuilder.append(exportOptions.isAppend());
                xmlWriterBuilder.encoding(fileOptions.getEncoding());
                xmlWriterBuilder.xmlObjectMarshaller(xmlMarshaller());
                xmlWriterBuilder.lineSeparator(exportOptions.getLineSeparator());
                xmlWriterBuilder.rootName(exportOptions.getRootName());
                xmlWriterBuilder.resource(resource);
                xmlWriterBuilder.saveState(false);
                log.info("Creating XML writer with {} for file {}", exportOptions, file);
                return xmlWriterBuilder.build();
            default:
                JsonResourceItemWriterBuilder<DataStructure<String>> jsonWriterBuilder = new JsonResourceItemWriterBuilder<>();
                jsonWriterBuilder.name("json-resource-item-writer");
                jsonWriterBuilder.append(exportOptions.isAppend());
                jsonWriterBuilder.encoding(fileOptions.getEncoding());
                jsonWriterBuilder.jsonObjectMarshaller(new JacksonJsonObjectMarshaller<>());
                jsonWriterBuilder.lineSeparator(exportOptions.getLineSeparator());
                jsonWriterBuilder.resource(resource);
                jsonWriterBuilder.saveState(false);
                log.info("Creating JSON writer with {} for file {}", exportOptions, file);
                return jsonWriterBuilder.build();
        }
    }

    private JsonObjectMarshaller<DataStructure<String>> xmlMarshaller() {
        XmlMapper mapper = new XmlMapper();
        mapper.setConfig(mapper.getSerializationConfig().withRootName(exportOptions.getElementName()));
        JacksonJsonObjectMarshaller<DataStructure<String>> marshaller = new JacksonJsonObjectMarshaller<>();
        marshaller.setObjectMapper(mapper);
        return marshaller;
    }

}
