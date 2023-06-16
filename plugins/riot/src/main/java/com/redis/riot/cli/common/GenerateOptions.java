package com.redis.riot.cli.common;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.redis.spring.batch.common.DoubleRange;
import com.redis.spring.batch.common.IntRange;
import com.redis.spring.batch.reader.GeneratorItemReader;
import com.redis.spring.batch.reader.GeneratorItemReader.CollectionOptions;
import com.redis.spring.batch.reader.GeneratorItemReader.HashOptions;
import com.redis.spring.batch.reader.GeneratorItemReader.JsonOptions;
import com.redis.spring.batch.reader.GeneratorItemReader.ListOptions;
import com.redis.spring.batch.reader.GeneratorItemReader.MapOptions;
import com.redis.spring.batch.reader.GeneratorItemReader.SetOptions;
import com.redis.spring.batch.reader.GeneratorItemReader.StreamOptions;
import com.redis.spring.batch.reader.GeneratorItemReader.StreamOptions.BodyOptions;
import com.redis.spring.batch.reader.GeneratorItemReader.StringOptions;
import com.redis.spring.batch.reader.GeneratorItemReader.TimeSeriesOptions;
import com.redis.spring.batch.reader.GeneratorItemReader.Type;
import com.redis.spring.batch.reader.GeneratorItemReader.ZsetOptions;

import picocli.CommandLine.Option;

public class GenerateOptions {

	public static final int DEFAULT_COUNT = 1000;
	public static final IntRange DEFAULT_KEY_RANGE = IntRange.between(1, 1000);

	@Option(names = "--count", description = "Number of items to generate (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int count = DEFAULT_COUNT;
	@Option(names = "--keyspace", description = "Keyspace prefix for generated data structures (default: ${DEFAULT-VALUE}).", paramLabel = "<str>")
	private String keyspace = GeneratorItemReader.DEFAULT_KEYSPACE;
	@Option(names = "--keys", description = "Start and end index for keys (default: ${DEFAULT-VALUE}).", paramLabel = "<range>")
	private IntRange keyRange = DEFAULT_KEY_RANGE;
	@Option(arity = "1..*", names = "--types", description = "Data structure types to generate: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<type>")
	private List<Type> types = GeneratorItemReader.defaultTypes();
	@Option(names = "--expiration", description = "TTL in seconds.", paramLabel = "<secs>")
	private Optional<IntRange> expiration = Optional.empty();
	@Option(names = "--hash-fields", description = "Number of fields in hashes (default: ${DEFAULT-VALUE}).", paramLabel = "<range>")
	private IntRange hashSize = MapOptions.DEFAULT_FIELD_COUNT;
	@Option(names = "--hash-field-size", description = "Value size for hash fields (default: ${DEFAULT-VALUE}).", paramLabel = "<range>")
	private IntRange hashFieldSize = MapOptions.DEFAULT_FIELD_LENGTH;
	@Option(names = "--json-size", description = "Number of fields in JSON docs (default: ${DEFAULT-VALUE}).", paramLabel = "<range>")
	private IntRange jsonSize = MapOptions.DEFAULT_FIELD_COUNT;
	@Option(names = "--json-field-size", description = "Value size for JSON fields (default: ${DEFAULT-VALUE}).", paramLabel = "<range>")
	private IntRange jsonFieldSize = MapOptions.DEFAULT_FIELD_LENGTH;
	@Option(names = "--list-card", description = "Number of elements in lists (default: ${DEFAULT-VALUE}).", paramLabel = "<range>")
	private IntRange listSize = CollectionOptions.DEFAULT_CARDINALITY;
	@Option(names = "--set-size", description = "Number of elements in sets (default: ${DEFAULT-VALUE}).", paramLabel = "<range>")
	private IntRange setSize = CollectionOptions.DEFAULT_CARDINALITY;
	@Option(names = "--stream-size", description = "Number of messages in streams (default: ${DEFAULT-VALUE}).", paramLabel = "<range>")
	private IntRange streamSize = StreamOptions.DEFAULT_MESSAGE_COUNT;
	@Option(names = "--stream-field-count", description = "Number of fields in stream messages (default: ${DEFAULT-VALUE}).", paramLabel = "<range>")
	private IntRange streamFieldCount = MapOptions.DEFAULT_FIELD_COUNT;
	@Option(names = "--stream-field-size", description = "Value size for fields in stream messages (default: ${DEFAULT-VALUE}).", paramLabel = "<range>")
	private IntRange streamFieldSize = MapOptions.DEFAULT_FIELD_LENGTH;
	@Option(names = "--string-size", description = "Length of strings (default: ${DEFAULT-VALUE}).", paramLabel = "<range>")
	private IntRange stringSize = StringOptions.DEFAULT_LENGTH;
	@Option(names = "--ts-size", description = "Number of samples in timeseries (default: ${DEFAULT-VALUE}).", paramLabel = "<range>")
	private IntRange timeseriesSize = TimeSeriesOptions.DEFAULT_SAMPLE_COUNT;
	@Option(names = "--ts-time", description = "Start time for samples in timeseries, e.g. 2007-12-03T10:15:30.00Z (default: now).", paramLabel = "<epoch>")
	private Optional<Instant> timeseriesStartTime = Optional.empty();
	@Option(names = "--zset-size", description = "Number of elements in sorted sets (default: ${DEFAULT-VALUE}).", paramLabel = "<range>")
	private IntRange zsetSize = CollectionOptions.DEFAULT_CARDINALITY;
	@Option(names = "--zset-score", description = "Score of sorted sets (default: ${DEFAULT-VALUE}).", paramLabel = "<range>")
	private DoubleRange zsetScore = ZsetOptions.DEFAULT_SCORE;

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public IntRange getKeyRange() {
		return keyRange;
	}

	public void setKeyRange(IntRange keyRange) {
		this.keyRange = keyRange;
	}

	public String getKeyspace() {
		return keyspace;
	}

	public void setKeyspace(String keyspace) {
		this.keyspace = keyspace;
	}

	public List<Type> getTypes() {
		return types;
	}

	public void setTypes(Type... types) {
		this.types = Stream.of(types).collect(Collectors.toList());
	}

	public Optional<IntRange> getExpiration() {
		return expiration;
	}

	public IntRange getHashFieldSize() {
		return hashFieldSize;
	}

	public IntRange getHashSize() {
		return hashSize;
	}

	public IntRange getJsonSize() {
		return jsonSize;
	}

	public IntRange getJsonFieldSize() {
		return jsonFieldSize;
	}

	public IntRange getStringSize() {
		return stringSize;
	}

	public void setStringSize(IntRange stringSize) {
		this.stringSize = stringSize;
	}

	public DoubleRange getZsetScore() {
		return zsetScore;
	}

	public void setZsetScore(DoubleRange zsetScore) {
		this.zsetScore = zsetScore;
	}

	public IntRange getStreamSize() {
		return streamSize;
	}

	public IntRange getStreamFieldCount() {
		return streamFieldCount;
	}

	public void setStreamFieldCount(IntRange streamFieldCount) {
		this.streamFieldCount = streamFieldCount;
	}

	public IntRange getStreamFieldSize() {
		return streamFieldSize;
	}

	public void setStreamFieldSize(IntRange streamFieldSize) {
		this.streamFieldSize = streamFieldSize;
	}

	public void setHashSize(IntRange hashSize) {
		this.hashSize = hashSize;
	}

	public void setHashFieldSize(IntRange hashFieldSize) {
		this.hashFieldSize = hashFieldSize;
	}

	public void setJsonSize(IntRange jsonSize) {
		this.jsonSize = jsonSize;
	}

	public void setJsonFieldSize(IntRange jsonFieldSize) {
		this.jsonFieldSize = jsonFieldSize;
	}

	public void setStreamSize(IntRange streamSize) {
		this.streamSize = streamSize;
	}

	public IntRange getSetSize() {
		return setSize;
	}

	public void setSetSize(IntRange setSize) {
		this.setSize = setSize;
	}

	public IntRange getZsetSize() {
		return zsetSize;
	}

	public void setZsetSize(IntRange zsetSize) {
		this.zsetSize = zsetSize;
	}

	public IntRange getListSize() {
		return listSize;
	}

	public void setListSize(IntRange listSize) {
		this.listSize = listSize;
	}

	public IntRange getTimeseriesSize() {
		return timeseriesSize;
	}

	public void setTimeseriesSize(IntRange timeseriesSize) {
		this.timeseriesSize = timeseriesSize;
	}

	public Optional<Instant> getTimeseriesStartTime() {
		return timeseriesStartTime;
	}

	public void setTimeseriesStartTime(Optional<Instant> timeseriesStartTime) {
		this.timeseriesStartTime = timeseriesStartTime;
	}

	public void setTypes(List<Type> types) {
		this.types = types;
	}

	public void setExpiration(Optional<IntRange> expiration) {
		this.expiration = expiration;
	}

	@Override
	public String toString() {
		return "DataStructureGeneratorOptions [keyspace=" + keyspace + ", types=" + types + ", expiration=" + expiration
				+ ", hashSize=" + hashSize + ", hashFieldSize=" + hashFieldSize + ", jsonSize=" + jsonSize
				+ ", jsonFieldSize=" + jsonFieldSize + ", listSize=" + listSize + ", setSize=" + setSize
				+ ", streamSize=" + streamSize + ", streamFieldCount=" + streamFieldCount + ", streamFieldSize="
				+ streamFieldSize + ", stringSize=" + stringSize + ", timeseriesSize=" + timeseriesSize
				+ ", timeseriesStartTime=" + timeseriesStartTime + ", zsetSize=" + zsetSize + ", zsetScore=" + zsetScore
				+ ", count=" + count + "]";
	}

	public SetOptions setOptions() {
		return SetOptions.builder().cardinality(setSize).build();
	}

	public ZsetOptions zsetOptions() {
		return ZsetOptions.builder().cardinality(zsetSize).score(zsetScore).build();
	}

	public HashOptions hashOptions() {
		return HashOptions.builder().fieldCount(hashSize).fieldLength(hashFieldSize).build();
	}

	public JsonOptions jsonOptions() {
		return JsonOptions.builder().fieldCount(jsonSize).fieldLength(jsonFieldSize).build();
	}

	public ListOptions listOptions() {
		return ListOptions.builder().cardinality(listSize).build();
	}

	public StreamOptions streamOptions() {
		return StreamOptions.builder()
				.bodyOptions(BodyOptions.builder().fieldCount(streamFieldCount).fieldLength(streamFieldSize).build())
				.messageCount(streamSize).build();
	}

	public StringOptions stringOptions() {
		return StringOptions.builder().length(stringSize).build();
	}

	public TimeSeriesOptions timeSeriesOptions() {
		TimeSeriesOptions.Builder builder = TimeSeriesOptions.builder().sampleCount(timeseriesSize);
		timeseriesStartTime.ifPresent(t -> builder.startTime(t.toEpochMilli()));
		return builder.build();
	}

}
