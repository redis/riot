package com.redislabs.riot.file;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redislabs.riot.AbstractExportCommand;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
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

import java.io.IOException;

@Slf4j
@Data
@EqualsAndHashCode(callSuper = true)
@Command(name = "export", description = "Export Redis data to JSON or XML files")
public class FileExportCommand extends AbstractExportCommand<DataStructure> {

    @CommandLine.Parameters(arity = "1", description = "File path or URL", paramLabel = "FILE")
    private String file;
    @CommandLine.ArgGroup(exclusive = false, heading = "File export options%n")
    private FileExportOptions options = new FileExportOptions();

    @Override
    protected Flow flow(StepBuilderFactory stepBuilderFactory) throws Exception {
        return flow(step(stepBuilderFactory,null, writer()).build());
    }

    private ItemWriter<DataStructure> writer() throws IOException {
        WritableResource resource = options.outputResource(file);
        DumpFileType fileType = fileType();
        if (fileType == DumpFileType.XML) {
            XmlResourceItemWriterBuilder<DataStructure> xmlWriterBuilder = new XmlResourceItemWriterBuilder<>();
            xmlWriterBuilder.name("xml-resource-item-writer");
            xmlWriterBuilder.append(options.isAppend());
            xmlWriterBuilder.encoding(options.getEncoding().name());
            xmlWriterBuilder.xmlObjectMarshaller(xmlMarshaller());
            xmlWriterBuilder.lineSeparator(options.getLineSeparator());
            xmlWriterBuilder.rootName(options.getRootName());
            xmlWriterBuilder.resource(resource);
            xmlWriterBuilder.saveState(false);
            log.debug("Creating XML writer with {} for file {}", options, file);
            return xmlWriterBuilder.build();
        }
        JsonResourceItemWriterBuilder<DataStructure> jsonWriterBuilder = new JsonResourceItemWriterBuilder<>();
        jsonWriterBuilder.name("json-resource-item-writer");
        jsonWriterBuilder.append(options.isAppend());
        jsonWriterBuilder.encoding(options.getEncoding().name());
        jsonWriterBuilder.jsonObjectMarshaller(new JacksonJsonObjectMarshaller<>());
        jsonWriterBuilder.lineSeparator(options.getLineSeparator());
        jsonWriterBuilder.resource(resource);
        jsonWriterBuilder.saveState(false);
        log.debug("Creating JSON writer with {} for file {}", options, file);
        return jsonWriterBuilder.build();
    }

    private DumpFileType fileType() {
        if (options.getType() == null) {
            return DumpFileType.of(file);
        }
        return options.getType();
    }

    private JsonObjectMarshaller<DataStructure> xmlMarshaller() {
        XmlMapper mapper = new XmlMapper();
        mapper.setConfig(mapper.getSerializationConfig().withRootName(options.getElementName()));
        JacksonJsonObjectMarshaller<DataStructure> marshaller = new JacksonJsonObjectMarshaller<>();
        marshaller.setObjectMapper(mapper);
        return marshaller;
    }

}
