package com.redis.riot.file;

import org.springframework.batch.item.ItemReader;
import org.springframework.core.io.Resource;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redis.riot.file.xml.XmlItemReaderBuilder;
import com.redis.riot.file.xml.XmlObjectReader;

public class XmlReaderFactory extends AbstractReaderFactory {

	@Override
	public ItemReader<?> create(Resource resource, ReadOptions options) {
		XmlItemReaderBuilder<Object> builder = new XmlItemReaderBuilder<>();
		builder.name(resource.getFilename() + "-xml-file-reader");
		builder.resource(resource);
		XmlObjectReader<Object> objectReader = new XmlObjectReader<>(options.getItemType());
		objectReader.setMapper(objectMapper(new XmlMapper(), options));
		builder.xmlObjectReader(objectReader);
		if (options.getMaxItemCount() > 0) {
			builder.maxItemCount(options.getMaxItemCount());
		}
		return builder.build();
	}

}
