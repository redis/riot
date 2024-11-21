package com.redis.riot.file;

import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.core.io.WritableResource;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redis.riot.file.xml.XmlResourceItemWriterBuilder;

public class XmlWriterFactory extends AbstractWriterFactory {

	@Override
	public ItemWriter<?> create(WritableResource resource, WriteOptions options) {
		XmlResourceItemWriterBuilder<?> writer = new XmlResourceItemWriterBuilder<>();
		writer.name(resource.getFilename());
		writer.append(options.isAppend());
		writer.encoding(options.getEncoding());
		writer.lineSeparator(options.getLineSeparator());
		writer.rootName(options.getRootName());
		writer.resource(resource);
		writer.saveState(false);
		XmlMapper mapper = objectMapper(new XmlMapper());
		mapper.setConfig(mapper.getSerializationConfig().withRootName(options.getElementName()));
		writer.xmlObjectMarshaller(new JacksonJsonObjectMarshaller<>(mapper));
		return writer.build();
	}

}
