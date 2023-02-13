package com.redis.riot.gen;

import static com.redis.spring.batch.reader.DataStructureGeneratorItemReader.DEFAULT_HASH_FIELD_SIZE;
import static com.redis.spring.batch.reader.DataStructureGeneratorItemReader.DEFAULT_HASH_SIZE;
import static com.redis.spring.batch.reader.DataStructureGeneratorItemReader.DEFAULT_JSON_FIELD_COUNT;
import static com.redis.spring.batch.reader.DataStructureGeneratorItemReader.DEFAULT_JSON_FIELD_SIZE;
import static com.redis.spring.batch.reader.DataStructureGeneratorItemReader.DEFAULT_KEYSPACE;
import static com.redis.spring.batch.reader.DataStructureGeneratorItemReader.DEFAULT_LIST_SIZE;
import static com.redis.spring.batch.reader.DataStructureGeneratorItemReader.DEFAULT_SET_SIZE;
import static com.redis.spring.batch.reader.DataStructureGeneratorItemReader.DEFAULT_STREAM_FIELD_COUNT;
import static com.redis.spring.batch.reader.DataStructureGeneratorItemReader.DEFAULT_STREAM_FIELD_SIZE;
import static com.redis.spring.batch.reader.DataStructureGeneratorItemReader.DEFAULT_STREAM_SIZE;
import static com.redis.spring.batch.reader.DataStructureGeneratorItemReader.DEFAULT_STRING_SIZE;
import static com.redis.spring.batch.reader.DataStructureGeneratorItemReader.DEFAULT_TIMESERIES_SIZE;
import static com.redis.spring.batch.reader.DataStructureGeneratorItemReader.DEFAULT_ZSET_SCORE;
import static com.redis.spring.batch.reader.DataStructureGeneratorItemReader.DEFAULT_ZSET_SIZE;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import com.redis.spring.batch.common.DataStructure.Type;
import com.redis.spring.batch.common.DoubleRange;
import com.redis.spring.batch.common.IntRange;
import com.redis.spring.batch.reader.DataStructureGeneratorItemReader;
import com.redis.spring.batch.reader.DataStructureGeneratorItemReader.Builder;

import picocli.CommandLine.Option;

public class GenerateOptions extends GeneratorOptions {

	@Option(names = "--keyspace", description = "Keyspace prefix for generated data structures (default: ${DEFAULT-VALUE}).", paramLabel = "<str>")
	private String keyspace = DEFAULT_KEYSPACE;
	@Option(arity = "1..*", names = "--types", description = "Data structure types to generate: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<type>")
	private List<Type> types = DataStructureGeneratorItemReader.defaultTypes();
	@Option(names = "--expiration", description = "TTL in seconds.", paramLabel = "<secs>")
	private Optional<IntRange> expiration = Optional.empty();
	@Option(names = "--hash-size", description = "Number of fields in hashes (default: ${DEFAULT-VALUE}).", paramLabel = "<range>")
	private IntRange hashSize = DEFAULT_HASH_SIZE;
	@Option(names = "--hash-field-size", description = "Value size for hash fields (default: ${DEFAULT-VALUE}).", paramLabel = "<range>")
	private IntRange hashFieldSize = DEFAULT_HASH_FIELD_SIZE;
	@Option(names = "--json-size", description = "Number of fields in JSON docs (default: ${DEFAULT-VALUE}).", paramLabel = "<range>")
	private IntRange jsonSize = DEFAULT_JSON_FIELD_COUNT;
	@Option(names = "--json-field-size", description = "Value size for JSON fields (default: ${DEFAULT-VALUE}).", paramLabel = "<range>")
	private IntRange jsonFieldSize = DEFAULT_JSON_FIELD_SIZE;
	@Option(names = "--list-size", description = "Number of elements in lists (default: ${DEFAULT-VALUE}).", paramLabel = "<range>")
	private IntRange listSize = DEFAULT_LIST_SIZE;
	@Option(names = "--set-size", description = "Number of elements in sets (default: ${DEFAULT-VALUE}).", paramLabel = "<range>")
	private IntRange setSize = DEFAULT_SET_SIZE;
	@Option(names = "--stream-size", description = "Number of messages in streams (default: ${DEFAULT-VALUE}).", paramLabel = "<range>")
	private IntRange streamSize = DEFAULT_STREAM_SIZE;
	@Option(names = "--stream-field-count", description = "Number of fields in stream messages (default: ${DEFAULT-VALUE}).", paramLabel = "<range>")
	private IntRange streamFieldCount = DEFAULT_STREAM_FIELD_COUNT;
	@Option(names = "--stream-field-size", description = "Value size for fields in stream messages (default: ${DEFAULT-VALUE}).", paramLabel = "<range>")
	private IntRange streamFieldSize = DEFAULT_STREAM_FIELD_SIZE;
	@Option(names = "--string-size", description = "Length of strings (default: ${DEFAULT-VALUE}).", paramLabel = "<range>")
	private IntRange stringSize = DEFAULT_STRING_SIZE;
	@Option(names = "--ts-size", description = "Number of samples in timeseries (default: ${DEFAULT-VALUE}).", paramLabel = "<range>")
	private IntRange timeseriesSize = DEFAULT_TIMESERIES_SIZE;
	@Option(names = "--ts-time", description = "Start time for samples in timeseries, e.g. 2007-12-03T10:15:30.00Z (default: now).", paramLabel = "<epoch>")
	private Optional<Instant> timeseriesStartTime = Optional.empty();
	@Option(names = "--zset-size", description = "Number of elements in sorted sets (default: ${DEFAULT-VALUE}).", paramLabel = "<range>")
	private IntRange zsetSize = DEFAULT_ZSET_SIZE;
	@Option(names = "--zset-score", description = "Score of sorted sets (default: ${DEFAULT-VALUE}).", paramLabel = "<range>")
	private DoubleRange zsetScore = DEFAULT_ZSET_SCORE;

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
		this.types = Arrays.asList(types);
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
				+ ", start=" + start + ", count=" + count + "]";
	}

	public Builder configureReader(Builder reader) {
		timeseriesStartTime.ifPresent(t -> reader.timeseriesStartTime(t.toEpochMilli()));
		expiration.ifPresent(reader::expiration);
		return reader;
	}

}
