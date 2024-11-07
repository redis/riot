package com.redis.riot.file.xml;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redis.riot.file.xml.XmlObjectReader;

import org.springframework.core.io.Resource;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;
import java.io.InputStream;

/**
 * 
 * @author Julien Ruaux
 *
 * @param <T> type of the target object
 */
public class XmlObjectReader<T> {

	private Class<? extends T> itemType;
	private XMLStreamReader reader;
	private XmlMapper mapper = new XmlMapper();
	private InputStream inputStream;

	/**
	 * Create a new {@link XmlObjectReader} instance.
	 * 
	 * @param itemType the target item type
	 */
	public XmlObjectReader(Class<? extends T> itemType) {
		this.itemType = itemType;
	}

	/**
	 * Set the object mapper to use to map Xml objects to domain objects.
	 * 
	 * @param mapper the object mapper to use
	 */
	public void setMapper(XmlMapper mapper) {
		Assert.notNull(mapper, "The mapper must not be null");
		this.mapper = mapper;
	}

	public void open(Resource resource) throws Exception {
		Assert.notNull(resource, "The resource must not be null");
		this.inputStream = resource.getInputStream();
		this.reader = XMLInputFactory.newFactory().createXMLStreamReader(this.inputStream);
		this.mapper = new XmlMapper();
		if (reader.hasNext()) {
			reader.next(); // point to root element
		} else {
			throw new Exception("XML not in the form <root><element>...</element></root>");
		}
		if (reader.hasNext()) {
			reader.next(); // point to first element under root
		} else {
			throw new Exception("XML not in the form <root><element>...</element></root>");
		}
	}

	@Nullable
	public T read() throws Exception {
		if (reader.hasNext()) {
			try {
				return mapper.readValue(reader, itemType);
			} catch (JsonParseException e) {
				// reached end of stream, ignore
			}
		}
		return null;
	}

	public void close() throws Exception {
		this.inputStream.close();
		this.reader.close();
	}

}
