/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.redis.riot.file.xml;

import java.util.Iterator;

import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.json.JsonObjectMarshaller;
import org.springframework.core.io.Resource;
import org.springframework.core.io.WritableResource;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redis.spring.batch.resource.AbstractFileItemWriter;

/**
 * Item writer that writes data in XML format to an output file. The location of
 * the output file is defined by a {@link Resource} and must represent a
 * writable file. Items are transformed to XML format using a {@link XmlMapper}.
 * Items will be enclosed in a XML element as follows:
 * 
 * <pre>
 * {@code
 * <root>
 *  <record>...</record>
 *  <record>...</record>
 *  <record>...</record>
 * </root>
 * }
 * </pre>
 * 
 * The implementation is <b>not</b> thread-safe.
 *
 */
public class XmlResourceItemWriter<T> extends AbstractFileItemWriter<T> {

	private JsonObjectMarshaller<T> xmlObjectMarshaller;

	/**
	 * Create a new {@link XmlResourceItemWriter} instance.
	 * 
	 * @param resource            to write XML data to
	 * @param rootName            XML root element tag name
	 * @param xmlObjectMarshaller used to marshal object into XML representation
	 */
	public XmlResourceItemWriter(WritableResource resource, String rootName,
			JsonObjectMarshaller<T> xmlObjectMarshaller) {
		Assert.notNull(resource, "resource must not be null");
		Assert.notNull(rootName, "root name must not be null");
		Assert.notNull(xmlObjectMarshaller, "xml object writer must not be null");
		setResource(resource);
		setRootName(rootName);
		setXmlObjectMarshaller(xmlObjectMarshaller);
		setExecutionContextName(ClassUtils.getShortName(XmlResourceItemWriter.class));
	}

	public void setRootName(String rootName) {
		setHeaderCallback(writer -> writer.write("<" + rootName + ">"));
		setFooterCallback(writer -> writer.write(this.lineSeparator + "</" + rootName + ">" + this.lineSeparator));
	}

	/**
	 * Assert that mandatory properties (xmlObjectMarshaller) are set.
	 *
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	@Override
	public void afterPropertiesSet() throws Exception {
		if (this.append) {
			this.shouldDeleteIfExists = false;
		}
	}

	/**
	 * Set the {@link JsonObjectMarshaller} to use to marshal object to XML.
	 * 
	 * @param objectMarshaller the marshaller to use
	 */
	public void setXmlObjectMarshaller(JsonObjectMarshaller<T> objectMarshaller) {
		this.xmlObjectMarshaller = objectMarshaller;
	}

	@Override
	public String doWrite(Chunk<? extends T> items) {
		StringBuilder lines = new StringBuilder();
		Iterator<? extends T> iterator = items.iterator();
		if (!items.isEmpty() && state.getLinesWritten() > 0) {
			lines.append(this.lineSeparator);
		}
		while (iterator.hasNext()) {
			T item = iterator.next();
			lines.append(' ').append(this.xmlObjectMarshaller.marshal(item));
			if (iterator.hasNext()) {
				lines.append(this.lineSeparator);
			}
		}
		return lines.toString();
	}

}
