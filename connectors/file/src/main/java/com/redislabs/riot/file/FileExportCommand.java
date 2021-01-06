package com.redislabs.riot.file;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redislabs.riot.AbstractExportCommand;
import lombok.Setter;
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

@Setter
@Command(name = "export", description = "Export Redis data to a JSON or XML file")
public class FileExportCommand extends AbstractExportCommand<DataStructure<String>> {

    @CommandLine.Parameters(arity = "1", description = "File path or URL", paramLabel = "FILE")
    protected String file;
    @Mixin
    private FileExportOptions options = new FileExportOptions();
    @Mixin
    private FileOptions fileOptions = new FileOptions();

    @Override
    protected Flow flow() throws Exception {
        return flow(step(null, writer()).build());
    }

    private ItemWriter<DataStructure<String>> writer() throws IOException {
        FileType fileType = FileUtils.fileType(file);
        WritableResource resource = FileUtils.outputResource(file, fileOptions);
        switch (fileType) {
            case JSON:
                JsonResourceItemWriterBuilder<DataStructure<String>> jsonWriterBuilder = new JsonResourceItemWriterBuilder<>();
                jsonWriterBuilder.name("json-resource-item-writer");
                jsonWriterBuilder.append(options.isAppend());
                jsonWriterBuilder.encoding(fileOptions.getEncoding());
                jsonWriterBuilder.jsonObjectMarshaller(new JacksonJsonObjectMarshaller<>());
                jsonWriterBuilder.lineSeparator(options.getLineSeparator());
                jsonWriterBuilder.resource(resource);
                jsonWriterBuilder.saveState(false);
                return jsonWriterBuilder.build();
            case XML:
                XmlResourceItemWriterBuilder<DataStructure<String>> xmlWriterBuilder = new XmlResourceItemWriterBuilder<>();
                xmlWriterBuilder.name("xml-resource-item-writer");
                xmlWriterBuilder.append(options.isAppend());
                xmlWriterBuilder.encoding(fileOptions.getEncoding());
                xmlWriterBuilder.xmlObjectMarshaller(xmlMarshaller());
                xmlWriterBuilder.lineSeparator(options.getLineSeparator());
                xmlWriterBuilder.rootName(options.getRootName());
                xmlWriterBuilder.resource(resource);
                xmlWriterBuilder.saveState(false);
                return xmlWriterBuilder.build();
            default:
                throw new IllegalArgumentException("Unsupported file type: " + fileType);
        }
    }

    private JsonObjectMarshaller<DataStructure<String>> xmlMarshaller() {
        XmlMapper mapper = new XmlMapper();
        mapper.setConfig(mapper.getSerializationConfig().withRootName(options.getElementName()));
        JacksonJsonObjectMarshaller<DataStructure<String>> marshaller = new JacksonJsonObjectMarshaller<>();
        marshaller.setObjectMapper(mapper);
        return marshaller;
    }

}
