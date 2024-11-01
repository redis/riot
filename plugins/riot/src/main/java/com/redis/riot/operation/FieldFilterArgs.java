package com.redis.riot.operation;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.springframework.util.ObjectUtils;

import com.redis.riot.core.processor.MapFilteringFunction;
import com.redis.riot.core.processor.MapFlatteningFunction;
import com.redis.riot.core.processor.ObjectToStringFunction;

import lombok.ToString;
import picocli.CommandLine.Option;

@ToString
public class FieldFilterArgs {

	@Option(arity = "1..*", names = "--include", description = "Fields to include.", paramLabel = "<field>")
	private List<String> includeFields;

	@Option(arity = "1..*", names = "--exclude", description = "Fields to exclude.", paramLabel = "<field>")
	private List<String> excludeFields;

	public Function<Map<String, Object>, Map<String, String>> mapFunction() {
		Function<Map<String, Object>, Map<String, String>> mapFlattener = new MapFlatteningFunction<>(
				new ObjectToStringFunction());
		if (ObjectUtils.isEmpty(includeFields) && ObjectUtils.isEmpty(excludeFields)) {
			return mapFlattener;
		}
		MapFilteringFunction filtering = new MapFilteringFunction();
		if (!ObjectUtils.isEmpty(includeFields)) {
			filtering.includes(includeFields);
		}
		if (!ObjectUtils.isEmpty(excludeFields)) {
			filtering.excludes(excludeFields);
		}
		return mapFlattener.andThen(filtering);
	}

	public List<String> getExcludeFields() {
		return excludeFields;
	}

	public void setExcludeFields(List<String> excludes) {
		this.excludeFields = excludes;
	}

	public List<String> getIncludeFields() {
		return includeFields;
	}

	public void setIncludeFields(List<String> includes) {
		this.includeFields = includes;
	}

}