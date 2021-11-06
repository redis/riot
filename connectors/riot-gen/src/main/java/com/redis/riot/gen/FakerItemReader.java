package com.redis.riot.gen;

import java.util.Map;

import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import lombok.Builder;

/**
 * {@link ItemReader} that generates HashMaps using Faker.
 *
 * @author Julien Ruaux
 */
public class FakerItemReader extends AbstractItemCountingItemStreamItemReader<Map<String, Object>> {

	private final long start;
	private final long end;
	private final Generator<Map<String, Object>> generator;

	@Builder
	private FakerItemReader(Generator<Map<String, Object>> generator, long start, long end) {
		setName(ClassUtils.getShortName(FakerItemReader.class));
		Assert.notNull(generator, "A generator is required");
		Assert.isTrue(end > start, "End index must be strictly greater than start index");
		setMaxItemCount(Math.toIntExact(end - start));
		this.generator = generator;
		this.start = start;
		this.end = end;
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
