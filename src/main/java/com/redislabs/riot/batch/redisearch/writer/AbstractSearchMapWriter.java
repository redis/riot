package com.redislabs.riot.batch.redisearch.writer;

import com.redislabs.lettusearch.search.AddOptions;

import lombok.Setter;
import lombok.experimental.Accessors;

@Accessors(fluent = true)
public abstract class AbstractSearchMapWriter<R> extends AbstractLettuSearchMapWriter<R> {

	@Setter
	protected AddOptions options;

	@Override
	public String toString() {
		return String.format("RediSearch index %s", index);
	}

}
