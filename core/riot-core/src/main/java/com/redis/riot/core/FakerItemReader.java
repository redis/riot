package com.redis.riot.core;

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

	public static final int DEFAULT_START = 1;
	public static final int DEFAULT_COUNT = 1000;

	private int start = DEFAULT_START;
	private int count = DEFAULT_COUNT;
	private final Generator<Map<String, Object>> generator;

	public FakerItemReader(Generator<Map<String, Object>> generator) {
		setName(ClassUtils.getShortName(FakerItemReader.class));
		Assert.notNull(generator, "A generator is required");
		setMaxItemCount(count);
		this.generator = generator;
	}

	public void setStart(int start) {
		this.start = start;
	}

	public void setCount(int count) {
		this.count = count;
		setMaxItemCount(count);
	}

	@Override
	protected Map<String, Object> doRead() throws Exception {
		return generator.next(start + ((getCurrentItemCount() - 1) % count));
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
