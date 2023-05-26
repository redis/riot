package com.redis.riot.cli.operation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;

import com.redis.lettucemod.timeseries.AddOptions;
import com.redis.lettucemod.timeseries.AddOptions.Builder;
import com.redis.lettucemod.timeseries.Label;
import com.redis.lettucemod.timeseries.Sample;
import com.redis.spring.batch.convert.SampleConverter;
import com.redis.spring.batch.writer.operation.TsAdd;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

@Command(name = "ts.add", description = "Add samples to RedisTimeSeries")
public class TsAddCommand extends AbstractKeyCommand {

	@Mixin
	private TsAddOptions options = new TsAddOptions();

	@Override
	public TsAdd<String, String, Map<String, Object>> operation() {
		return new TsAdd<>(key(), sample(), this::addOptions);
	}

	private Function<Map<String, Object>, Sample> sample() {
		return new SampleConverter<>(numberExtractor(options.getTimestampField(), Long.class, null),
				numberExtractor(options.getValueField(), Double.class));
	}

	@SuppressWarnings("unchecked")
	private AddOptions<String, String> addOptions(Map<String, Object> source) {
		Builder<String, String> builder = AddOptions.<String, String>builder().policy(options.getDuplicatePolicy());
		if (!options.getLabels().isEmpty()) {
			List<Label<String, String>> labelList = new ArrayList<>();
			for (Entry<String, String> label : options.getLabels().entrySet()) {
				if (source.containsKey(label.getValue())) {
					labelList.add(Label.of(label.getKey(), String.valueOf(source.get(label.getValue()))));
				}
			}
			builder.labels(labelList.toArray(new Label[0]));
		}
		return builder.build();
	}
}
