package org.springframework.batch.item.xml.builder;

import org.springframework.batch.item.xml.XmlItemReader;
import org.springframework.batch.item.xml.XmlObjectReader;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

public class XmlItemReaderBuilder<T> {

	private XmlObjectReader<T> xmlObjectReader;

	private Resource resource;

	private String name;

	private boolean strict = true;

	private boolean saveState = true;

	private int maxItemCount = Integer.MAX_VALUE;

	private int currentItemCount;

	/**
	 * Set the {@link XmlObjectReader} to use to read and map XML objects to domain
	 * objects.
	 * 
	 * @param xmlObjectReader to use
	 * @return The current instance of the builder.
	 * @see XmlItemReader#setXmlObjectReader(XmlObjectReader)
	 */
	public XmlItemReaderBuilder<T> xmlObjectReader(XmlObjectReader<T> xmlObjectReader) {
		this.xmlObjectReader = xmlObjectReader;
		return this;
	}

	/**
	 * The {@link Resource} to be used as input.
	 * 
	 * @param resource the input to the reader.
	 * @return The current instance of the builder.
	 * @see XmlItemReader#setResource(Resource)
	 */
	public XmlItemReaderBuilder<T> resource(Resource resource) {
		this.resource = resource;
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
	public XmlItemReaderBuilder<T> name(String name) {
		this.name = name;

		return this;
	}

	/**
	 * Setting this value to true indicates that it is an error if the input does
	 * not exist and an exception will be thrown. Defaults to true.
	 * 
	 * @param strict indicates the input resource must exist
	 * @return The current instance of the builder.
	 * @see XmlItemReader#setStrict(boolean)
	 */
	public XmlItemReaderBuilder<T> strict(boolean strict) {
		this.strict = strict;

		return this;
	}

	/**
	 * Configure if the state of the
	 * {@link org.springframework.batch.item.ItemStreamSupport} should be persisted
	 * within the {@link org.springframework.batch.item.ExecutionContext} for
	 * restart purposes.
	 * 
	 * @param saveState defaults to true
	 * @return The current instance of the builder.
	 */
	public XmlItemReaderBuilder<T> saveState(boolean saveState) {
		this.saveState = saveState;

		return this;
	}

	/**
	 * Configure the max number of items to be read.
	 * 
	 * @param maxItemCount the max items to be read
	 * @return The current instance of the builder.
	 * @see org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader#setMaxItemCount(int)
	 */
	public XmlItemReaderBuilder<T> maxItemCount(int maxItemCount) {
		this.maxItemCount = maxItemCount;

		return this;
	}

	/**
	 * Index for the current item. Used on restarts to indicate where to start from.
	 * 
	 * @param currentItemCount current index
	 * @return The current instance of the builder.
	 * @see org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader#setCurrentItemCount(int)
	 */
	public XmlItemReaderBuilder<T> currentItemCount(int currentItemCount) {
		this.currentItemCount = currentItemCount;

		return this;
	}

	/**
	 * Validate the configuration and build a new {@link XmlItemReader}.
	 * 
	 * @return a new instance of the {@link XmlItemReader}
	 */
	public XmlItemReader<T> build() {
		Assert.notNull(this.xmlObjectReader, "A XML object reader is required.");
		Assert.notNull(this.resource, "A resource is required.");
		if (this.saveState) {
			Assert.state(StringUtils.hasText(this.name), "A name is required when saveState is set to true.");
		}

		XmlItemReader<T> reader = new XmlItemReader<>(this.resource, this.xmlObjectReader);
		reader.setName(this.name);
		reader.setStrict(this.strict);
		reader.setSaveState(this.saveState);
		reader.setMaxItemCount(this.maxItemCount);
		reader.setCurrentItemCount(this.currentItemCount);

		return reader;
	}
}
