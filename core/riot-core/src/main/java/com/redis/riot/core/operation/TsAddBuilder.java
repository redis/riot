package com.redis.riot.core.operation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;
import java.util.function.ToLongFunction;

import org.springframework.util.CollectionUtils;

import com.redis.lettucemod.timeseries.AddOptions;
import com.redis.lettucemod.timeseries.AddOptions.Builder;
import com.redis.lettucemod.timeseries.DuplicatePolicy;
import com.redis.lettucemod.timeseries.Label;
import com.redis.lettucemod.timeseries.Sample;
import com.redis.spring.batch.common.ToSampleFunction;
import com.redis.spring.batch.writer.operation.AbstractKeyWriteOperation;
import com.redis.spring.batch.writer.operation.TsAdd;

public class TsAddBuilder extends AbstractMapOperationBuilder {

	public static final DuplicatePolicy DEFAULT_DUPLICATE_POLICY = DuplicatePolicy.LAST;

	private String timestampField;

	private String valueField;

	private DuplicatePolicy duplicatePolicy = DEFAULT_DUPLICATE_POLICY;

	private Map<String, String> labels;

	@Override
	protected AbstractKeyWriteOperation<String, String, Map<String, Object>> operation(
			Function<Map<String, Object>, String> keyFunction) {
		TsAdd<String, String, Map<String, Object>> operation = new TsAdd<>(keyFunction, sample());
		operation.setOptionsFunction(this::addOptions);
		return operation;
	}

	private Function<Map<String, Object>, Sample> sample() {
		ToLongFunction<Map<String, Object>> timestamp = toLong(timestampField, 0);
		ToDoubleFunction<Map<String, Object>> value = toDouble(valueField, 0);
		return new ToSampleFunction<>(timestamp, value);
	}

	@SuppressWarnings("unchecked")
	private AddOptions<String, String> addOptions(Map<String, Object> source) {
		Builder<String, String> builder = AddOptions.<String, String>builder().policy(duplicatePolicy);
		if (!CollectionUtils.isEmpty(labels)) {
			List<Label<String, String>> labelList = new ArrayList<>();
			for (Entry<String, String> label : labels.entrySet()) {
				if (source.containsKey(label.getValue())) {
					labelList.add(Label.of(label.getKey(), String.valueOf(source.get(label.getValue()))));
				}
			}
			builder.labels(labelList.toArray(new Label[0]));
		}
		return builder.build();
	}

	public void setTimestampField(String field) {
		this.timestampField = field;
	}

	public void setValueField(String field) {
		this.valueField = field;
	}

	public void setDuplicatePolicy(DuplicatePolicy policy) {
		this.duplicatePolicy = policy;
	}

	public void setLabels(Map<String, String> labels) {
		this.labels = labels;
	}

}
