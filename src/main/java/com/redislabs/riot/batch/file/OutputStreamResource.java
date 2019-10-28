/*
 * Copyright 2002-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.redislabs.riot.batch.file;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.springframework.core.io.AbstractResource;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.core.io.WritableResource;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * {@link Resource} implementation for a given {@link InputStream}.
 * <p>
 * Should only be used if no other specific {@code Resource} implementation is
 * applicable. In particular, prefer {@link ByteArrayResource} or any of the
 * file-based {@code Resource} implementations where possible.
 *
 * <p>
 * In contrast to other {@code Resource} implementations, this is a descriptor
 * for an <i>already opened</i> resource - therefore returning {@code true} from
 * {@link #isOpen()}. Do not use an {@code InputStreamResource} if you need to
 * keep the resource descriptor somewhere, or if you need to read from a stream
 * multiple times.
 *
 * @author Juergen Hoeller
 * @author Sam Brannen
 * @since 28.12.2003
 * @see ByteArrayResource
 * @see ClassPathResource
 * @see FileSystemResource
 * @see UrlResource
 */
public class OutputStreamResource extends AbstractResource implements WritableResource {

	private final OutputStream outputStream;

	private final String description;

	/**
	 * Create a new InputStreamResource.
	 * 
	 * @param inputStream the InputStream to use
	 */
	public OutputStreamResource(OutputStream outputStream) {
		this(outputStream, "resource loaded through OutputStream");
	}

	/**
	 * Create a new InputStreamResource.
	 * 
	 * @param inputStream the InputStream to use
	 * @param description where the InputStream comes from
	 */
	public OutputStreamResource(OutputStream outputStream, @Nullable String description) {
		Assert.notNull(outputStream, "InputStream must not be null");
		this.outputStream = outputStream;
		this.description = (description != null ? description : "");
	}

	/**
	 * This implementation always returns {@code true}.
	 */
	@Override
	public boolean exists() {
		return true;
	}

	/**
	 * This implementation always returns {@code true}.
	 */
	@Override
	public boolean isOpen() {
		return true;
	}

	/**
	 * This implementation throws IllegalStateException if attempting to read the
	 * underlying stream multiple times.
	 */
	@Override
	public OutputStream getOutputStream() throws IOException, IllegalStateException {
		return this.outputStream;
	}

	/**
	 * This implementation returns a description that includes the passed-in
	 * description, if any.
	 */
	@Override
	public String getDescription() {
		return "OutputStream resource [" + this.description + "]";
	}

	/**
	 * This implementation compares the underlying InputStream.
	 */
	@Override
	public boolean equals(Object other) {
		return (this == other || (other instanceof OutputStreamResource
				&& ((OutputStreamResource) other).outputStream.equals(this.outputStream)));
	}

	/**
	 * This implementation returns the hash code of the underlying InputStream.
	 */
	@Override
	public int hashCode() {
		return this.outputStream.hashCode();
	}

	@Override
	public InputStream getInputStream() {
		throw new UnsupportedOperationException("getInputStream() not supported for OutputStream resource");
	}

}
