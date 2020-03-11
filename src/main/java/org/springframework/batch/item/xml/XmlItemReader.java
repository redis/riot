package org.springframework.batch.item.xml;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.file.ResourceAwareItemReaderItemStream;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.core.io.Resource;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * {@link ItemStreamReader} implementation that reads XML objects from a
 * {@link Resource} having the following format:
 * <p>
 * <code>
 * <root>
 *    <item>
 *       // XML object
 *    </item>
 *    <item>
 *       // XML object
 *    </item>
 * ]
 * </root>
 * <p>
 *
 * The implementation is <b>not</b> thread-safe.
 *
 * @param <T> the type of XML objects to read
 *
 * @author Julien Ruaux
 */
public class XmlItemReader<T> extends AbstractItemCountingItemStreamItemReader<T>
		implements ResourceAwareItemReaderItemStream<T> {

	private static final Log LOGGER = LogFactory.getLog(XmlItemReader.class);

	private Resource resource;

	private XmlObjectReader<T> xmlObjectReader;

	private boolean strict = true;

	/**
	 * Create a new {@link XmlItemReader} instance.
	 * 
	 * @param resource         the input XML resource
	 * @param xmlObjectReader the XML object reader to use
	 */
	public XmlItemReader(Resource resource, XmlObjectReader<T> xmlObjectReader) {
		Assert.notNull(resource, "The resource must not be null.");
		Assert.notNull(xmlObjectReader, "The XML object reader must not be null.");
		this.resource = resource;
		this.xmlObjectReader = xmlObjectReader;
	}

	/**
	 * Set the {@link XmlObjectReader} to use to read and map XML elements to
	 * domain objects.
	 * 
	 * @param xmlObjectReader the XML object reader to use
	 */
	public void setXmlObjectReader(XmlObjectReader<T> xmlObjectReader) {
		this.xmlObjectReader = xmlObjectReader;
	}

	/**
	 * In strict mode the reader will throw an exception on
	 * {@link #open(org.springframework.batch.item.ExecutionContext)} if the input
	 * resource does not exist.
	 * 
	 * @param strict true by default
	 */
	public void setStrict(boolean strict) {
		this.strict = strict;
	}

	@Override
	public void setResource(Resource resource) {
		this.resource = resource;
	}

	@Nullable
	@Override
	protected T doRead() throws Exception {
		return xmlObjectReader.read();
	}

	@Override
	protected void doOpen() throws Exception {
		if (!this.resource.exists()) {
			if (this.strict) {
				throw new IllegalStateException("Input resource must exist (reader is in 'strict' mode)");
			}
			LOGGER.warn("Input resource does not exist " + this.resource.getDescription());
			return;
		}
		if (!this.resource.isReadable()) {
			if (this.strict) {
				throw new IllegalStateException("Input resource must be readable (reader is in 'strict' mode)");
			}
			LOGGER.warn("Input resource is not readable " + this.resource.getDescription());
			return;
		}
		this.xmlObjectReader.open(this.resource);
	}

	@Override
	protected void doClose() throws Exception {
		this.xmlObjectReader.close();
	}

}
