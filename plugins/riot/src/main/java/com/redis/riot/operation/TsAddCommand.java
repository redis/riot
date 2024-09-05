package com.redis.riot.operation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
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
import com.redis.lettucemod.timeseries.Sample;
import com.redis.riot.function.ToSample;
import com.redis.spring.batch.item.redis.writer.impl.TsAdd;

import io.lettuce.core.KeyValue;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "ts.add", description = "Add samples to RedisTimeSeries")
public class TsAddCommand extends AbstractOperationCommand {

	public static final DuplicatePolicy DEFAULT_DUPLICATE_POLICY = DuplicatePolicy.LAST;

	@Option(names = "--value", required = true, description = "Name of the field to use for values.", paramLabel = "<field>")
	private String valueField;

	@Option(names = "--timestamp", description = "Name of the field to use for timestamps. If unset, uses auto-timestamping.", paramLabel = "<field>")
	private String timestampField;

	@Option(names = "--on-duplicate", description = "Duplicate policy: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<name>")
	private DuplicatePolicy duplicatePolicy = DEFAULT_DUPLICATE_POLICY;

	@Option(arity = "1..*", names = "--label", description = "Labels in the form label1=field1 label2=field2...", paramLabel = "SPEL")
	private Map<String, String> labels = new LinkedHashMap<>();

	@Override
	public TsAdd<String, String, Map<String, Object>> operation() {
		TsAdd<String, String, Map<String, Object>> operation = new TsAdd<>(keyFunction(), valueFunction());
		operation.setOptionsFunction(this::addOptions);
		return operation;
	}

	@SuppressWarnings("unchecked")
	private AddOptions<String, String> addOptions(Map<String, Object> source) {
		Builder<String, String> builder = AddOptions.<String, String>builder().policy(duplicatePolicy);
		if (!CollectionUtils.isEmpty(labels)) {
			List<KeyValue<String, String>> labelList = new ArrayList<>();
			for (Entry<String, String> label : labels.entrySet()) {
				if (source.containsKey(label.getValue())) {
					labelList.add(KeyValue.just(label.getKey(), String.valueOf(source.get(label.getValue()))));
				}
			}
			builder.labels(labelList.toArray(new KeyValue[0]));
		}
		return builder.build();
	}

	private Function<Map<String, Object>, Collection<Sample>> valueFunction() {
		ToLongFunction<Map<String, Object>> timestamp = toLong(timestampField);
		ToDoubleFunction<Map<String, Object>> value = toDouble(valueField, 0);
		return new ToSample<>(timestamp, value).andThen(Arrays::asList);
	}

	public String getTimestampField() {
		return timestampField;
	}

	public void setTimestampField(String timestampField) {
		this.timestampField = timestampField;
	}

	public String getValueField() {
		return valueField;
	}

	public void setValueField(String valueField) {
		this.valueField = valueField;
	}

	public DuplicatePolicy getDuplicatePolicy() {
		return duplicatePolicy;
	}

	public void setDuplicatePolicy(DuplicatePolicy duplicatePolicy) {
		this.duplicatePolicy = duplicatePolicy;
	}

	public Map<String, String> getLabels() {
		return labels;
	}

	public void setLabels(Map<String, String> labels) {
		this.labels = labels;
	}

}