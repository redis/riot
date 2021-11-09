package com.redis.riot.gen;

import java.util.Map;

import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * {@link ItemReader} that generates HashMaps using Faker.
 *
 * @author Julien Ruaux
 */
public class FakerItemReader extends AbstractItemCountingItemStreamItemReader<Map<String, Object>> {

	public static final long DEFAULT_START = 0;
	public static final long DEFAULT_END = 1000;

	private long start = DEFAULT_START;
	private long end = DEFAULT_END;
	private final Generator<Map<String, Object>> generator;

	public FakerItemReader(Generator<Map<String, Object>> generator) {
		setName(ClassUtils.getShortName(FakerItemReader.class));
		Assert.notNull(generator, "A generator is required");
		setMaxItemCount();
		this.generator = generator;
	}

	public void setStart(long start) {
		this.start = start;
		setMaxItemCount();
	}

	public void setEnd(long end) {
		this.end = end;
		setMaxItemCount();
	}

	private void setMaxItemCount() {
		setMaxItemCount(Math.toIntExact(end - start));
	}

	@Override
	protected Map<String, Object> doRead() throws Exception {
		return generator.next(start + (getCurrentItemCount() % (end - start)));
	}

	@Override
	protected void doOpen() throws Exception {
		// nothing to see here, move along
	}

	@Override
	protected void doClose() throws Exception {
		// nothing to see here, move along
	}

}
