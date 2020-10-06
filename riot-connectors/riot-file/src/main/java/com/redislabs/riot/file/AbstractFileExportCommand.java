package com.redislabs.riot.file;

import java.io.IOException;

import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.batch.item.redis.support.KeyValue;
import org.springframework.batch.item.resource.JsonResourceItemWriter;
import org.springframework.batch.item.resource.JsonResourceItemWriterBuilder;
import org.springframework.batch.item.xml.StaxEventItemWriter;
import org.springframework.batch.item.xml.builder.StaxEventItemWriterBuilder;
import org.springframework.core.io.WritableResource;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;

import com.redislabs.riot.AbstractExportCommand;

import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command
public abstract class AbstractFileExportCommand<O> extends AbstractExportCommand<O> {

	@CommandLine.Parameters(arity = "1", description = "File path or URL", paramLabel = "FILE")
	protected String file;
	@CommandLine.Mixin
	protected FileOptions fileOptions = new FileOptions();
	@CommandLine.Option(names = "--append", description = "Append to file if it exists")
	protected boolean append;
	@CommandLine.Option(names = "--root", description = "XML root element tag name", paramLabel = "<string>")
	protected String root;
	@CommandLine.Option(names = "--line-sep", description = "String to separate lines (default: system default)", paramLabel = "<string>")
	protected String lineSeparator = FlatFileItemWriter.DEFAULT_LINE_SEPARATOR;

	@Override
	protected ItemWriter<O> writer() throws IOException {
		FileType fileType = fileOptions.fileType(file);
		WritableResource resource = fileOptions.outputResource(file);
		return writer(fileType, resource);
	}

	protected abstract ItemWriter<O> writer(FileType fileType, WritableResource resource) throws IOException;

	protected <T> JsonResourceItemWriter<T> jsonWriter(WritableResource resource) {
		JsonResourceItemWriterBuilder<T> jsonWriterBuilder = new JsonResourceItemWriterBuilder<>();
		jsonWriterBuilder.name("json-resource-item-writer");
		jsonWriterBuilder.append(append);
		jsonWriterBuilder.encoding(fileOptions.getEncoding());
		jsonWriterBuilder.jsonObjectMarshaller(new JacksonJsonObjectMarshaller<>());
		jsonWriterBuilder.lineSeparator(lineSeparator);
		jsonWriterBuilder.resource(resource);
		jsonWriterBuilder.saveState(false);
		return jsonWriterBuilder.build();
	}

	protected <T> StaxEventItemWriter<T> xmlWriter(WritableResource resource) {
		StaxEventItemWriterBuilder<T> xmlWriterBuilder = new StaxEventItemWriterBuilder<>();
		xmlWriterBuilder.name("xml-resource-item-writer");
		xmlWriterBuilder.encoding(fileOptions.getEncoding());
		Jaxb2Marshaller marshaller = new Jaxb2Marshaller();
		marshaller.setClassesToBeBound(KeyValue.class);
		xmlWriterBuilder.marshaller(marshaller);
		xmlWriterBuilder.rootTagName(root);
		xmlWriterBuilder.resource(resource);
		xmlWriterBuilder.saveState(false);
		return xmlWriterBuilder.build();
	}

}
