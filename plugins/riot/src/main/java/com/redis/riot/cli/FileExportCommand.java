package com.redis.riot.cli;

import java.io.IOException;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.job.builder.JobBuilderException;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.batch.item.json.JsonObjectMarshaller;
import org.springframework.core.io.WritableResource;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redis.riot.core.FileDumpType;
import com.redis.riot.core.resource.JsonResourceItemWriterBuilder;
import com.redis.riot.core.resource.XmlResourceItemWriterBuilder;
import com.redis.spring.batch.common.DataStructure;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Parameters;

@Command(name = "file-export", description = "Export Redis data to JSON or XML files")
public class FileExportCommand extends AbstractExportCommand {

	private static final String COMMAND_NAME = "file-export";

	@Parameters(arity = "1", description = "File path or URL", paramLabel = "FILE")
	private String file;

	@Mixin
	private FileDumpOptions dumpFileOptions = new FileDumpOptions();

	@ArgGroup(exclusive = false, heading = "File export options%n")
	private FileExportOptions options = new FileExportOptions();

	public String getFile() {
		return file;
	}

	public void setFile(String file) {
		this.file = file;
	}

	public FileExportOptions getOptions() {
		return options;
	}

	public void setOptions(FileExportOptions options) {
		this.options = options;
	}

	@Override
	protected Job job(JobCommandContext context) {
		WritableResource resource;
		try {
			resource = options.outputResource(file);
		} catch (IOException e) {
			throw new JobBuilderException(e);
		}
		String task = String.format("Exporting %s", resource.getFilename());
		return job(context, COMMAND_NAME, step(context, COMMAND_NAME, reader(context), null, writer(resource)), task);
	}

	private ItemWriter<DataStructure<String>> writer(WritableResource resource) {
		FileDumpType type = dumpFileOptions.type(resource);
		switch (type) {
		case XML:
			XmlResourceItemWriterBuilder<DataStructure<String>> xmlWriterBuilder = new XmlResourceItemWriterBuilder<>();
			xmlWriterBuilder.name("xml-resource-item-writer");
			xmlWriterBuilder.append(options.isAppend());
			xmlWriterBuilder.encoding(options.getEncoding().name());
			xmlWriterBuilder.xmlObjectMarshaller(xmlMarshaller());
			xmlWriterBuilder.lineSeparator(options.getLineSeparator());
			xmlWriterBuilder.rootName(options.getRootName());
			xmlWriterBuilder.resource(resource);
			xmlWriterBuilder.saveState(false);
			return xmlWriterBuilder.build();
		case JSON:
			JsonResourceItemWriterBuilder<DataStructure<String>> jsonWriterBuilder = new JsonResourceItemWriterBuilder<>();
			jsonWriterBuilder.name("json-resource-item-writer");
			jsonWriterBuilder.append(options.isAppend());
			jsonWriterBuilder.encoding(options.getEncoding().name());
			jsonWriterBuilder.jsonObjectMarshaller(new JacksonJsonObjectMarshaller<>());
			jsonWriterBuilder.lineSeparator(options.getLineSeparator());
			jsonWriterBuilder.resource(resource);
			jsonWriterBuilder.saveState(false);
			return jsonWriterBuilder.build();
		default:
			throw new UnsupportedOperationException("Unsupported file type: " + type);
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
