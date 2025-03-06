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

import org.springframework.batch.item.file.FlatFileFooterCallback;
import org.springframework.batch.item.file.FlatFileHeaderCallback;
import org.springframework.batch.item.json.JsonObjectMarshaller;
import org.springframework.core.io.Resource;
import org.springframework.core.io.WritableResource;
import org.springframework.util.Assert;

import com.redis.spring.batch.resource.AbstractFileItemWriter;

/**
 * Builder for {@link XmlResourceItemWriter}.
 *
 * @param <T> type of objects to write as XML output.
 */
public class XmlResourceItemWriterBuilder<T> {

	private WritableResource resource;
	private String rootName;
	private JsonObjectMarshaller<T> xmlObjectMarshaller;
	private FlatFileHeaderCallback headerCallback;
	private FlatFileFooterCallback footerCallback;

	private String name;
	private String encoding = AbstractFileItemWriter.DEFAULT_CHARSET;
	private String lineSeparator = AbstractFileItemWriter.DEFAULT_LINE_SEPARATOR;

	private boolean append = false;
	private boolean saveState = true;
	private boolean shouldDeleteIfExists = true;
	private boolean shouldDeleteIfEmpty = false;

	/**
	 * Configure if the state of the
	 * {@link org.springframework.batch.item.ItemStreamSupport} should be persisted
	 * within the {@link org.springframework.batch.item.ExecutionContext} for
	 * restart purposes.
	 *
	 * @param saveState defaults to true
	 * @return The current instance of the builder.
	 */
	public XmlResourceItemWriterBuilder<T> saveState(boolean saveState) {
		this.saveState = saveState;

		return this;
	}

	/**
	 * The name used to calculate the key within the
	 * {@link org.springframework.batch.item.ExecutionContext}. Required if
	 * {@link #saveState(boolean)} is set to true.
	 *
	 * @param name name of the reader instance
	 * @return The current instance of the builder.
	 * @see org.springframework.batch.item.ItemStreamSupport#setName(String)
	 */
	public XmlResourceItemWriterBuilder<T> name(String name) {
		this.name = name;

		return this;
	}

	/**
	 * String used to separate lines in output. Defaults to the System property
	 * <code>line.separator</code>.
	 *
	 * @param lineSeparator value to use for a line separator
	 * @return The current instance of the builder.
	 * @see XmlResourceItemWriter#setLineSeparator(String)
	 */
	public XmlResourceItemWriterBuilder<T> lineSeparator(String lineSeparator) {
		this.lineSeparator = lineSeparator;

		return this;
	}

	/**
	 * Set the {@link JsonObjectMarshaller} to use to marshal objects to XML.
	 *
	 * @param xmlObjectMarshaller to use
	 * @return The current instance of the builder.
	 * @see XmlResourceItemWriter#setXmlObjectMarshaller(JsonObjectMarshaller)
	 */
	public XmlResourceItemWriterBuilder<T> xmlObjectMarshaller(JsonObjectMarshaller<T> xmlObjectMarshaller) {
		this.xmlObjectMarshaller = xmlObjectMarshaller;

		return this;
	}

	/**
	 * The {@link Resource} to be used as output.
	 *
	 * @param resource the output of the writer.
	 * @return The current instance of the builder.
	 */
	public XmlResourceItemWriterBuilder<T> resource(WritableResource resource) {
		this.resource = resource;

		return this;
	}

	public XmlResourceItemWriterBuilder<T> rootName(String rootName) {
		this.rootName = rootName;
		return this;
	}

	/**
	 * Encoding used for output.
	 *
	 * @param encoding encoding type.
	 * @return The current instance of the builder.
	 * @see XmlResourceItemWriter#setEncoding(String)
	 */
	public XmlResourceItemWriterBuilder<T> encoding(String encoding) {
		this.encoding = encoding;

		return this;
	}

	/**
	 * If set to true, once the step is complete, if the resource previously
	 * provided is empty, it will be deleted.
	 *
	 * @param shouldDelete defaults to false
	 * @return The current instance of the builder
	 * @see XmlResourceItemWriter#setShouldDeleteIfEmpty(boolean)
	 */
	public XmlResourceItemWriterBuilder<T> shouldDeleteIfEmpty(boolean shouldDelete) {
		this.shouldDeleteIfEmpty = shouldDelete;

		return this;
	}

	/**
	 * If set to true, upon the start of the step, if the resource already exists,
	 * it will be deleted and recreated.
	 *
	 * @param shouldDelete defaults to true
	 * @return The current instance of the builder
	 * @see XmlResourceItemWriter#setShouldDeleteIfExists(boolean)
	 */
	public XmlResourceItemWriterBuilder<T> shouldDeleteIfExists(boolean shouldDelete) {
		this.shouldDeleteIfExists = shouldDelete;

		return this;
	}

	/**
	 * If set to true and the file exists, the output will be appended to the
	 * existing file.
	 *
	 * @param append defaults to false
	 * @return The current instance of the builder
	 * @see XmlResourceItemWriter#setAppendAllowed(boolean)
	 */
	public XmlResourceItemWriterBuilder<T> append(boolean append) {
		this.append = append;

		return this;
	}

	/**
	 * A callback for header processing.
	 *
	 * @param callback {@link FlatFileHeaderCallback} implementation
	 * @return The current instance of the builder
	 * @see XmlResourceItemWriter#setHeaderCallback(FlatFileHeaderCallback)
	 */
	public XmlResourceItemWriterBuilder<T> headerCallback(FlatFileHeaderCallback callback) {
		this.headerCallback = callback;

		return this;
	}

	/**
	 * A callback for footer processing.
	 *
	 * @param callback {@link FlatFileFooterCallback} implementation
	 * @return The current instance of the builder
	 * @see XmlResourceItemWriter#setFooterCallback(FlatFileFooterCallback)
	 */
	public XmlResourceItemWriterBuilder<T> footerCallback(FlatFileFooterCallback callback) {
		this.footerCallback = callback;

		return this;
	}

	/**
	 * Validate the configuration and build a new {@link XmlResourceItemWriter}.
	 *
	 * @return a new instance of the {@link XmlResourceItemWriter}
	 */
	public XmlResourceItemWriter<T> build() {
		Assert.notNull(this.resource, "A resource is required.");
		Assert.notNull(this.rootName, "A root name is required.");
		Assert.notNull(this.xmlObjectMarshaller, "An xml object marshaller is required.");

		if (this.saveState) {
			Assert.hasText(this.name, "A name is required when saveState is true");
		}

		XmlResourceItemWriter<T> xmlResourceItemWriter = new XmlResourceItemWriter<>(this.resource, this.rootName,
				this.xmlObjectMarshaller);

		xmlResourceItemWriter.setName(this.name);
		xmlResourceItemWriter.setAppendAllowed(this.append);
		xmlResourceItemWriter.setEncoding(this.encoding);
		if (this.headerCallback != null) {
			xmlResourceItemWriter.setHeaderCallback(this.headerCallback);
		}
		if (this.footerCallback != null) {
			xmlResourceItemWriter.setFooterCallback(this.footerCallback);
		}
		xmlResourceItemWriter.setLineSeparator(this.lineSeparator);
		xmlResourceItemWriter.setSaveState(this.saveState);
		xmlResourceItemWriter.setShouldDeleteIfEmpty(this.shouldDeleteIfEmpty);
		xmlResourceItemWriter.setShouldDeleteIfExists(this.shouldDeleteIfExists);
		return xmlResourceItemWriter;
	}
}
