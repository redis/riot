package com.redis.riot.redis;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import org.springframework.core.convert.converter.Converter;

import com.redis.lettucemod.timeseries.AddOptions;
import com.redis.lettucemod.timeseries.AddOptions.Builder;
import com.redis.lettucemod.timeseries.DuplicatePolicy;
import com.redis.lettucemod.timeseries.Label;
import com.redis.spring.batch.convert.SampleConverter;
import com.redis.spring.batch.writer.RedisOperation;
import com.redis.spring.batch.writer.operation.TsAdd;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "ts.add", description = "Add samples to RedisTimeSeries")
public class TsAddCommand extends AbstractKeyCommand {

	@Option(names = "--timestamp", description = "Name of the field to use for timestamps. If unset, uses auto-timestamping", paramLabel = "<field>")
	private Optional<String> timestampField = Optional.empty();
	@Option(names = "--value", required = true, description = "Name of the field to use for values.", paramLabel = "<field>")
	private String valueField;
	@Option(names = "--on-duplicate", description = "Duplicate policy: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<name>")
	private DuplicatePolicy duplicatePolicy = DuplicatePolicy.LAST;
	@Option(arity = "1..*", names = "--labels", description = "Labels in the form label1=field1 label2=field2...", paramLabel = "SPEL")
	private Map<String, String> labels = new LinkedHashMap<>();

	@Override
	public RedisOperation<String, String, Map<String, Object>> operation() {
		return TsAdd.<String, String, Map<String, Object>>key(key())
				.sample(new SampleConverter<>(numberExtractor(timestampField, Long.class, null),
						numberExtractor(valueField, Double.class)))
				.options(new AddOptionsConverter()).build();
	}

	private class AddOptionsConverter implements Converter<Map<String, Object>, AddOptions<String, String>> {

		@SuppressWarnings("unchecked")
		@Override
		public AddOptions<String, String> convert(Map<String, Object> source) {
			Builder<String, String> options = AddOptions.<String, String>builder().policy(duplicatePolicy);
			if (!labels.isEmpty()) {
				List<Label<String, String>> labelList = new ArrayList<>();
				for (Entry<String, String> label : labels.entrySet()) {
					if (source.containsKey(label.getValue())) {
						labelList.add(Label.of(label.getKey(), String.valueOf(source.get(label.getValue()))));
					}
				}
				options.labels(labelList.toArray(Label[]::new));
			}
			return options.build();
		}

	}
}
