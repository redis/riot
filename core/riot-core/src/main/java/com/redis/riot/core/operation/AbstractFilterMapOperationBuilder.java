package com.redis.riot.core.operation;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.springframework.util.ObjectUtils;

import com.redis.riot.core.function.MapFilteringFunction;
import com.redis.riot.core.function.MapFlatteningFunction;
import com.redis.riot.core.function.ObjectToStringFunction;

public abstract class AbstractFilterMapOperationBuilder extends AbstractMapOperationBuilder {

	private List<String> includes;
	private List<String> excludes;

	public void setIncludes(List<String> includes) {
		this.includes = includes;
	}

	public void setExcludes(List<String> excludes) {
		this.excludes = excludes;
	}

	protected Function<Map<String, Object>, Map<String, String>> map() {
		Function<Map<String, Object>, Map<String, String>> mapFlattener = new MapFlatteningFunction<>(
				new ObjectToStringFunction());
		if (ObjectUtils.isEmpty(includes) && ObjectUtils.isEmpty(excludes)) {
			return mapFlattener;
		}
		MapFilteringFunction filtering = new MapFilteringFunction();
		if (!ObjectUtils.isEmpty(includes)) {
			filtering.includes(includes);
		}
		if (!ObjectUtils.isEmpty(excludes)) {
			filtering.excludes(excludes);
		}
		return mapFlattener.andThen(filtering);
	}

}
