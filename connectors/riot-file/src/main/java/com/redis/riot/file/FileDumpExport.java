package com.redis.riot.file;

import java.io.IOException;

import org.springframework.batch.core.Job;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.batch.item.json.JsonFileItemWriter;
import org.springframework.batch.item.json.JsonObjectMarshaller;
import org.springframework.batch.item.json.builder.JsonFileItemWriterBuilder;
import org.springframework.core.io.Resource;
import org.springframework.core.io.WritableResource;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redis.riot.core.AbstractExport;
import com.redis.riot.file.xml.XmlResourceItemWriter;
import com.redis.riot.file.xml.XmlResourceItemWriterBuilder;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.reader.MemKeyValue;

import io.lettuce.core.codec.StringCodec;

public class FileDumpExport extends AbstractExport {

	public static final String DEFAULT_ELEMENT_NAME = "record";
	public static final String DEFAULT_ROOT_NAME = "root";
	public static final String DEFAULT_LINE_SEPARATOR = System.getProperty("line.separator");

	private String file;
	private FileOptions fileOptions = new FileOptions();
	private boolean append;
	private String rootName = DEFAULT_ROOT_NAME;
	private String elementName = DEFAULT_ELEMENT_NAME;
	private String lineSeparator = DEFAULT_LINE_SEPARATOR;
	private FileDumpType type;

	private ItemWriter<MemKeyValue<String, Object>> writer() {
		WritableResource resource;
		try {
			resource = FileUtils.outputResource(file, fileOptions);
		} catch (IOException e) {
			throw new ItemStreamException("Could not open file for writing: " + file, e);
		}
		if (dumpType(resource) == FileDumpType.XML) {
			return xmlWriter(resource);
		}
		return jsonWriter(resource);
	}

	private FileDumpType dumpType(WritableResource resource) {
		if (type == null) {
			return FileUtils.dumpType(resource);
		}
		return type;
	}

	private JsonFileItemWriter<MemKeyValue<String, Object>> jsonWriter(WritableResource resource) {
		JsonFileItemWriterBuilder<MemKeyValue<String, Object>> jsonWriterBuilder = new JsonFileItemWriterBuilder<>();
		jsonWriterBuilder.name("json-resource-item-writer");
		jsonWriterBuilder.append(append);
		jsonWriterBuilder.encoding(fileOptions.getEncoding());
		jsonWriterBuilder.jsonObjectMarshaller(new JacksonJsonObjectMarshaller<>(FileUtils.objectMapper()));
		jsonWriterBuilder.lineSeparator(lineSeparator);
		jsonWriterBuilder.resource(resource);
		jsonWriterBuilder.saveState(false);
		return jsonWriterBuilder.build();
	}

	private XmlResourceItemWriter<MemKeyValue<String, Object>> xmlWriter(Resource resource) {
		XmlResourceItemWriterBuilder<MemKeyValue<String, Object>> xmlWriterBuilder = new XmlResourceItemWriterBuilder<>();
		xmlWriterBuilder.name("xml-resource-item-writer");
		xmlWriterBuilder.append(append);
		xmlWriterBuilder.encoding(fileOptions.getEncoding());
		xmlWriterBuilder.xmlObjectMarshaller(xmlMarshaller());
		xmlWriterBuilder.lineSeparator(lineSeparator);
		xmlWriterBuilder.rootName(rootName);
		xmlWriterBuilder.resource(resource);
		xmlWriterBuilder.saveState(false);
		return xmlWriterBuilder.build();
	}

	private JsonObjectMarshaller<MemKeyValue<String, Object>> xmlMarshaller() {
		XmlMapper mapper = FileUtils.xmlMapper();
		mapper.setConfig(mapper.getSerializationConfig().withRootName(elementName));
		JacksonJsonObjectMarshaller<MemKeyValue<String, Object>> marshaller = new JacksonJsonObjectMarshaller<>();
		marshaller.setObjectMapper(mapper);
		return marshaller;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	protected Job job() {
		RedisItemReader<String, String, MemKeyValue<String, Object>> reader = RedisItemReader.struct();
		configure(reader);
		ItemProcessor processor = processor(StringCodec.UTF8);
		return jobBuilder().start(step(getName(), reader, writer()).processor(processor).build()).build();
	}

	public void setFile(String file) {
		this.file = file;
	}

	public void setFileOptions(FileOptions fileOptions) {
		this.fileOptions = fileOptions;
	}

	public void setType(FileDumpType type) {
		this.type = type;
	}

	public void setAppend(boolean append) {
		this.append = append;
	}

	public void setRootName(String rootName) {
		this.rootName = rootName;
	}

	public void setElementName(String elementName) {
		this.elementName = elementName;
	}

	public void setLineSeparator(String lineSeparator) {
		this.lineSeparator = lineSeparator;
	}

}
