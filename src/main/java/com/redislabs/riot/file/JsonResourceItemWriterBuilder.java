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

package com.redislabs.riot.file;

import org.springframework.batch.item.file.FlatFileFooterCallback;
import org.springframework.batch.item.file.FlatFileHeaderCallback;
import org.springframework.batch.item.json.JsonFileItemWriter;
import org.springframework.batch.item.json.JsonObjectMarshaller;
import org.springframework.core.io.Resource;
import org.springframework.core.io.WritableResource;
import org.springframework.util.Assert;

/**
 * Builder for {@link JsonFileItemWriter}.
 *
 * @param <T> type of objects to write as Json output.
 * @author Mahmoud Ben Hassine
 * @since 4.1
 */
public class JsonResourceItemWriterBuilder<T> {

	private WritableResource resource;
	private JsonObjectMarshaller<T> jsonObjectMarshaller;
	private FlatFileHeaderCallback headerCallback;
	private FlatFileFooterCallback footerCallback;

	private String name;
	private String encoding = JsonFileItemWriter.DEFAULT_CHARSET;
	private String lineSeparator = JsonFileItemWriter.DEFAULT_LINE_SEPARATOR;

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
	public JsonResourceItemWriterBuilder<T> saveState(boolean saveState) {
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
	public JsonResourceItemWriterBuilder<T> name(String name) {
		this.name = name;

		return this;
	}

	/**
	 * String used to separate lines in output. Defaults to the System property
	 * <code>line.separator</code>.
	 *
	 * @param lineSeparator value to use for a line separator
	 * @return The current instance of the builder.
	 * @see JsonFileItemWriter#setLineSeparator(String)
	 */
	public JsonResourceItemWriterBuilder<T> lineSeparator(String lineSeparator) {
		this.lineSeparator = lineSeparator;

		return this;
	}

	/**
	 * Set the {@link JsonObjectMarshaller} to use to marshal objects to json.
	 *
	 * @param jsonObjectMarshaller to use
	 * @return The current instance of the builder.
	 * @see JsonFileItemWriter#setJsonObjectMarshaller(JsonObjectMarshaller)
	 */
	public JsonResourceItemWriterBuilder<T> jsonObjectMarshaller(JsonObjectMarshaller<T> jsonObjectMarshaller) {
		this.jsonObjectMarshaller = jsonObjectMarshaller;

		return this;
	}

	/**
	 * The {@link Resource} to be used as output.
	 *
	 * @param resource the output of the writer.
	 * @return The current instance of the builder.
	 * @see JsonFileItemWriter#setResource(Resource)
	 */
	public JsonResourceItemWriterBuilder<T> resource(Resource resource) {
		Assert.isInstanceOf(WritableResource.class, resource);
		this.resource = (WritableResource) resource;

		return this;
	}

	/**
	 * Encoding used for output.
	 *
	 * @param encoding encoding type.
	 * @return The current instance of the builder.
	 * @see JsonFileItemWriter#setEncoding(String)
	 */
	public JsonResourceItemWriterBuilder<T> encoding(String encoding) {
		this.encoding = encoding;

		return this;
	}

	/**
	 * If set to true, once the step is complete, if the resource previously
	 * provided is empty, it will be deleted.
	 *
	 * @param shouldDelete defaults to false
	 * @return The current instance of the builder
	 * @see JsonFileItemWriter#setShouldDeleteIfEmpty(boolean)
	 */
	public JsonResourceItemWriterBuilder<T> shouldDeleteIfEmpty(boolean shouldDelete) {
		this.shouldDeleteIfEmpty = shouldDelete;

		return this;
	}

	/**
	 * If set to true, upon the start of the step, if the resource already exists,
	 * it will be deleted and recreated.
	 *
	 * @param shouldDelete defaults to true
	 * @return The current instance of the builder
	 * @see JsonFileItemWriter#setShouldDeleteIfExists(boolean)
	 */
	public JsonResourceItemWriterBuilder<T> shouldDeleteIfExists(boolean shouldDelete) {
		this.shouldDeleteIfExists = shouldDelete;

		return this;
	}

	/**
	 * If set to true and the file exists, the output will be appended to the
	 * existing file.
	 *
	 * @param append defaults to false
	 * @return The current instance of the builder
	 * @see JsonFileItemWriter#setAppendAllowed(boolean)
	 */
	public JsonResourceItemWriterBuilder<T> append(boolean append) {
		this.append = append;

		return this;
	}

	/**
	 * A callback for header processing.
	 *
	 * @param callback {@link FlatFileHeaderCallback} implementation
	 * @return The current instance of the builder
	 * @see JsonFileItemWriter#setHeaderCallback(FlatFileHeaderCallback)
	 */
	public JsonResourceItemWriterBuilder<T> headerCallback(FlatFileHeaderCallback callback) {
		this.headerCallback = callback;

		return this;
	}

	/**
	 * A callback for footer processing.
	 *
	 * @param callback {@link FlatFileFooterCallback} implementation
	 * @return The current instance of the builder
	 * @see JsonFileItemWriter#setFooterCallback(FlatFileFooterCallback)
	 */
	public JsonResourceItemWriterBuilder<T> footerCallback(FlatFileFooterCallback callback) {
		this.footerCallback = callback;

		return this;
	}

	/**
	 * Validate the configuration and build a new {@link JsonFileItemWriter}.
	 *
	 * @return a new instance of the {@link JsonFileItemWriter}
	 */
	public JsonResourceItemWriter<T> build() {
		Assert.notNull(this.resource, "A resource is required.");
		Assert.notNull(this.jsonObjectMarshaller, "A json object marshaller is required.");

		if (this.saveState) {
			Assert.hasText(this.name, "A name is required when saveState is true");
		}

		JsonResourceItemWriter<T> jsonFileItemWriter = new JsonResourceItemWriter<>(this.resource,
				this.jsonObjectMarshaller);

		jsonFileItemWriter.setName(this.name);
		jsonFileItemWriter.setAppendAllowed(this.append);
		jsonFileItemWriter.setEncoding(this.encoding);
		if (this.headerCallback != null) {
			jsonFileItemWriter.setHeaderCallback(this.headerCallback);
		}
		if (this.footerCallback != null) {
			jsonFileItemWriter.setFooterCallback(this.footerCallback);
		}
		jsonFileItemWriter.setLineSeparator(this.lineSeparator);
		jsonFileItemWriter.setSaveState(this.saveState);
		jsonFileItemWriter.setShouldDeleteIfEmpty(this.shouldDeleteIfEmpty);
		jsonFileItemWriter.setShouldDeleteIfExists(this.shouldDeleteIfExists);
		return jsonFileItemWriter;
	}
}
