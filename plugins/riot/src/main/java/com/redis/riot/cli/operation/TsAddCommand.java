package com.redis.riot.cli.operation;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Function;

import com.redis.lettucemod.timeseries.AddOptions;
import com.redis.lettucemod.timeseries.AddOptions.Builder;
import com.redis.lettucemod.timeseries.DuplicatePolicy;
import com.redis.lettucemod.timeseries.Label;
import com.redis.lettucemod.timeseries.Sample;
import com.redis.spring.batch.convert.SampleConverter;
import com.redis.spring.batch.writer.operation.TsAdd;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "ts.add", description = "Add samples to RedisTimeSeries")
public class TsAddCommand extends AbstractKeyCommand {

	public static final DuplicatePolicy DEFAULT_DUPLICATE_POLICY = DuplicatePolicy.LAST;

	@Option(names = "--timestamp", description = "Name of the field to use for timestamps. If unset, uses auto-timestamping.", paramLabel = "<field>")
	private Optional<String> timestampField = Optional.empty();
	@Option(names = "--value", required = true, description = "Name of the field to use for values.", paramLabel = "<field>")
	private String valueField;
	@Option(names = "--on-duplicate", description = "Duplicate policy: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<name>")
	private DuplicatePolicy duplicatePolicy = DEFAULT_DUPLICATE_POLICY;
	@Option(arity = "1..*", names = "--labels", description = "Labels in the form label1=field1 label2=field2...", paramLabel = "SPEL")
	private Map<String, String> labels = new LinkedHashMap<>();

	@Override
	public TsAdd<String, String, Map<String, Object>> operation() {
		return new TsAdd<>(key(), sample(), this::addOptions);
	}

	private Function<Map<String, Object>, Sample> sample() {
		return new SampleConverter<>(longExtractor(timestampField, 0), doubleExtractor(Optional.of(valueField), 0));
	}

	@SuppressWarnings("unchecked")
	private AddOptions<String, String> addOptions(Map<String, Object> source) {
		Builder<String, String> builder = AddOptions.<String, String>builder().policy(duplicatePolicy);
		if (!labels.isEmpty()) {
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
}
