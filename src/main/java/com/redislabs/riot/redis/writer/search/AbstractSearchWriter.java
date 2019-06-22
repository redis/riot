package com.redislabs.riot.redis.writer.search;

import com.redislabs.lettusearch.search.AddOptions;

import lombok.Setter;

public abstract class AbstractSearchWriter extends AbstractLettuSearchItemWriter {

	@Setter
	protected AddOptions options;

}
