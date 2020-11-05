package com.redislabs.riot.file;

import java.io.IOException;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.batch.item.json.JsonObjectMarshaller;
import org.springframework.batch.item.redis.support.DataStructure;
import org.springframework.batch.item.resource.support.JsonResourceItemWriterBuilder;
import org.springframework.batch.item.xml.support.XmlResourceItemWriterBuilder;
import org.springframework.core.io.WritableResource;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redislabs.riot.AbstractExportCommand;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "export", description = "Export Redis data to a JSON or XML file")
public class FileExportCommand extends AbstractExportCommand<DataStructure<String>> {

    @Parameters(arity = "1", description = "File path or URL", paramLabel = "FILE")
    protected String file;
    @Mixin
    protected FileOptions fileOptions = new FileOptions();
    @Option(names = "--append", description = "Append to file if it exists")
    private boolean append;
    @Option(names = "--root", description = "XML root element tag name (default: ${DEFAULT-VALUE})", paramLabel = "<string>")
    private String rootName = "root";
    @Option(names = "--element", description = "XML element tag name (default: ${DEFAULT-VALUE})", paramLabel = "<string>")
    private String elementName = "record";
    @Option(names = "--line-sep", description = "String to separate lines (default: system default)", paramLabel = "<string>")
    private String lineSeparator = FlatFileItemWriter.DEFAULT_LINE_SEPARATOR;

    @Override
    protected ItemWriter<DataStructure<String>> writer() throws IOException {
	FileType fileType = fileOptions.fileType(file);
	WritableResource resource = fileOptions.outputResource(file);
	switch (fileType) {
	case JSON:
	    JsonResourceItemWriterBuilder<DataStructure<String>> jsonWriterBuilder = new JsonResourceItemWriterBuilder<>();
	    jsonWriterBuilder.name("json-resource-item-writer");
	    jsonWriterBuilder.append(append);
	    jsonWriterBuilder.encoding(fileOptions.getEncoding());
	    jsonWriterBuilder.jsonObjectMarshaller(new JacksonJsonObjectMarshaller<>());
	    jsonWriterBuilder.lineSeparator(lineSeparator);
	    jsonWriterBuilder.resource(resource);
	    jsonWriterBuilder.saveState(false);
	    return jsonWriterBuilder.build();
	case XML:
	    XmlResourceItemWriterBuilder<DataStructure<String>> xmlWriterBuilder = new XmlResourceItemWriterBuilder<>();
	    xmlWriterBuilder.name("xml-resource-item-writer");
	    xmlWriterBuilder.append(append);
	    xmlWriterBuilder.encoding(fileOptions.getEncoding());
	    xmlWriterBuilder.xmlObjectMarshaller(xmlMarshaller());
	    xmlWriterBuilder.lineSeparator(lineSeparator);
	    xmlWriterBuilder.rootName(rootName);
	    xmlWriterBuilder.resource(resource);
	    xmlWriterBuilder.saveState(false);
	    return xmlWriterBuilder.build();
	default:
	    throw new IllegalArgumentException("Unsupported file type: " + fileType);
	}
    }

    private JsonObjectMarshaller<DataStructure<String>> xmlMarshaller() {
	XmlMapper mapper = new XmlMapper();
	mapper.setConfig(mapper.getSerializationConfig().withRootName(elementName));
	JacksonJsonObjectMarshaller<DataStructure<String>> marshaller = new JacksonJsonObjectMarshaller<>();
	marshaller.setObjectMapper(mapper);
	return marshaller;
    }

    @Override
    protected ItemProcessor<DataStructure<String>, DataStructure<String>> processor() {
	return null;
    }

}
