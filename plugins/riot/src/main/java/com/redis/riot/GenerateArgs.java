package com.redis.riot;

import java.time.Instant;
import java.util.List;

import com.redis.spring.batch.item.redis.common.Range;
import com.redis.spring.batch.item.redis.gen.CollectionOptions;
import com.redis.spring.batch.item.redis.gen.GeneratorItemReader;
import com.redis.spring.batch.item.redis.gen.ItemType;
import com.redis.spring.batch.item.redis.gen.MapOptions;
import com.redis.spring.batch.item.redis.gen.StreamOptions;
import com.redis.spring.batch.item.redis.gen.StringOptions;
import com.redis.spring.batch.item.redis.gen.TimeSeriesOptions;
import com.redis.spring.batch.item.redis.gen.ZsetOptions;

import lombok.ToString;
import picocli.CommandLine.Option;

@ToString
public class GenerateArgs {

	public static final int DEFAULT_COUNT = 1000;

	@Option(names = "--count", description = "Number of items to generate (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int count = DEFAULT_COUNT;

	@Option(names = "--key-separator", description = "Key separator (default: ${DEFAULT-VALUE}).", paramLabel = "<str>")
	private String keySepataror = GeneratorItemReader.DEFAULT_KEY_SEPARATOR;

	@Option(names = "--keyspace", description = "Keyspace prefix for generated data structures (default: ${DEFAULT-VALUE}).", paramLabel = "<str>")
	private String keyspace = GeneratorItemReader.DEFAULT_KEYSPACE;

	@Option(names = "--key-range", description = "Range of keys to generate in the form 'start-end' (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private Range keyRange = GeneratorItemReader.DEFAULT_KEY_RANGE;

	@Option(arity = "1..*", names = "--type", description = "Types of data structures to generate: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<type>")
	private List<ItemType> types = GeneratorItemReader.defaultTypes();

	@Option(names = "--index", description = "Name of index to create that matches JSON or hash type.", paramLabel = "<name>")
	private String index;

	@Option(names = "--expiration", description = "TTL in seconds.", paramLabel = "<secs>")
	private Range expiration;

	@Option(names = "--hash-size", description = "Number of fields in hashes (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private Range hashFieldCount = MapOptions.DEFAULT_FIELD_COUNT;

	@Option(names = "--hash-value", description = "Value size for hash fields (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private Range hashFieldLength = MapOptions.DEFAULT_FIELD_LENGTH;

	@Option(names = "--json-size", description = "Number of fields in JSON docs (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private Range jsonFieldCount = MapOptions.DEFAULT_FIELD_COUNT;

	@Option(names = "--json-value", description = "Value size for JSON fields (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private Range jsonFieldLength = MapOptions.DEFAULT_FIELD_LENGTH;

	@Option(names = "--list-size", description = "Number of elements in lists (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private Range listMemberCount = CollectionOptions.DEFAULT_MEMBER_COUNT;

	@Option(names = "--list-value", description = "Value size for list elements (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private Range listMemberRange = CollectionOptions.DEFAULT_MEMBER_RANGE;

	@Option(names = "--set-size", description = "Number of elements in sets (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private Range setMemberCount = CollectionOptions.DEFAULT_MEMBER_COUNT;

	@Option(names = "--set-value", description = "Value size for set elements (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private Range setMemberLength = CollectionOptions.DEFAULT_MEMBER_RANGE;

	@Option(names = "--stream-size", description = "Number of messages in streams (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private Range streamMessageCount = StreamOptions.DEFAULT_MESSAGE_COUNT;

	@Option(names = "--stream-fields", description = "Number of fields in stream messages (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private Range streamFieldCount = MapOptions.DEFAULT_FIELD_COUNT;

	@Option(names = "--stream-value", description = "Value size for fields in stream messages (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private Range streamFieldLength = MapOptions.DEFAULT_FIELD_LENGTH;

	@Option(names = "--string-value", description = "Length of strings (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private Range stringLength = StringOptions.DEFAULT_LENGTH;

	@Option(names = "--ts-size", description = "Number of samples in timeseries (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private Range timeseriesSampleCount = TimeSeriesOptions.DEFAULT_SAMPLE_COUNT;

	@Option(names = "--ts-time", description = "Start time for samples in timeseries, e.g. 2007-12-03T10:15:30.00Z (default: now).", paramLabel = "<epoch>")
	private Instant timeseriesStartTime;

	@Option(names = "--zset-size", description = "Number of elements in sorted sets (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private Range zsetMemberCount = CollectionOptions.DEFAULT_MEMBER_COUNT;

	@Option(names = "--zset-value", description = "Value size for sorted-set elements (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private Range zsetMemberLength = CollectionOptions.DEFAULT_MEMBER_RANGE;

	@Option(names = "--zset-score", description = "Score of sorted sets (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private Range zsetScore = ZsetOptions.DEFAULT_SCORE;

	private ZsetOptions zsetOptions() {
		ZsetOptions options = new ZsetOptions();
		options.setMemberCount(zsetMemberCount);
		options.setMemberRange(zsetMemberLength);
		options.setScore(zsetScore);
		return options;
	}

	private TimeSeriesOptions timeseriesOptions() {
		TimeSeriesOptions options = new TimeSeriesOptions();
		options.setSampleCount(timeseriesSampleCount);
		if (timeseriesStartTime != null) {
			options.setStartTime(timeseriesStartTime);
		}
		return options;
	}

	private StringOptions stringOptions() {
		StringOptions options = new StringOptions();
		options.setLength(stringLength);
		return options;
	}

	private StreamOptions streamOptions() {
		StreamOptions options = new StreamOptions();
		options.setBodyOptions(mapOptions(streamFieldCount, streamFieldLength));
		options.setMessageCount(streamMessageCount);
		return options;
	}

	private CollectionOptions setOptions() {
		return collectionOptions(setMemberCount, setMemberLength);
	}

	private CollectionOptions listOptions() {
		return collectionOptions(listMemberCount, listMemberRange);
	}

	private CollectionOptions collectionOptions(Range memberCount, Range memberRange) {
		CollectionOptions options = new CollectionOptions();
		options.setMemberCount(memberCount);
		options.setMemberRange(memberRange);
		return options;
	}

	private MapOptions jsonOptions() {
		return mapOptions(jsonFieldCount, jsonFieldLength);
	}

	private MapOptions hashOptions() {
		return mapOptions(hashFieldCount, hashFieldLength);
	}

	private MapOptions mapOptions(Range fieldCount, Range fieldLength) {
		MapOptions options = new MapOptions();
		options.setFieldCount(fieldCount);
		options.setFieldLength(fieldLength);
		return options;
	}

	public void configure(GeneratorItemReader reader) {
		reader.setKeySeparator(keySepataror);
		reader.setExpiration(expiration);
		reader.setHashOptions(hashOptions());
		reader.setJsonOptions(jsonOptions());
		reader.setKeyRange(keyRange);
		reader.setKeyspace(keyspace);
		reader.setListOptions(listOptions());
		reader.setSetOptions(setOptions());
		reader.setStreamOptions(streamOptions());
		reader.setStringOptions(stringOptions());
		reader.setTimeSeriesOptions(timeseriesOptions());
		reader.setTypes(types);
		reader.setZsetOptions(zsetOptions());
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public String getKeyspace() {
		return keyspace;
	}

	public void setKeyspace(String keyspace) {
		this.keyspace = keyspace;
	}

	public Range getKeyRange() {
		return keyRange;
	}

	public void setKeyRange(Range keyRange) {
		this.keyRange = keyRange;
	}

	public List<ItemType> getTypes() {
		return types;
	}

	public void setTypes(List<ItemType> types) {
		this.types = types;
	}

	public Range getExpiration() {
		return expiration;
	}

	public void setExpiration(Range expiration) {
		this.expiration = expiration;
	}

	public Range getHashFieldCount() {
		return hashFieldCount;
	}

	public void setHashFieldCount(Range hashFieldCount) {
		this.hashFieldCount = hashFieldCount;
	}

	public Range getHashFieldLength() {
		return hashFieldLength;
	}

	public void setHashFieldLength(Range hashFieldLength) {
		this.hashFieldLength = hashFieldLength;
	}

	public Range getJsonFieldCount() {
		return jsonFieldCount;
	}

	public void setJsonFieldCount(Range jsonFieldCount) {
		this.jsonFieldCount = jsonFieldCount;
	}

	public Range getJsonFieldLength() {
		return jsonFieldLength;
	}

	public void setJsonFieldLength(Range jsonFieldLength) {
		this.jsonFieldLength = jsonFieldLength;
	}

	public Range getListMemberCount() {
		return listMemberCount;
	}

	public void setListMemberCount(Range listMemberCount) {
		this.listMemberCount = listMemberCount;
	}

	public Range getListMemberRange() {
		return listMemberRange;
	}

	public void setListMemberRange(Range listMemberRange) {
		this.listMemberRange = listMemberRange;
	}

	public Range getSetMemberCount() {
		return setMemberCount;
	}

	public void setSetMemberCount(Range setMemberCount) {
		this.setMemberCount = setMemberCount;
	}

	public Range getSetMemberLength() {
		return setMemberLength;
	}

	public void setSetMemberLength(Range setMemberLength) {
		this.setMemberLength = setMemberLength;
	}

	public Range getStreamMessageCount() {
		return streamMessageCount;
	}

	public void setStreamMessageCount(Range streamMessageCount) {
		this.streamMessageCount = streamMessageCount;
	}

	public Range getStreamFieldCount() {
		return streamFieldCount;
	}

	public void setStreamFieldCount(Range streamFieldCount) {
		this.streamFieldCount = streamFieldCount;
	}

	public Range getStreamFieldLength() {
		return streamFieldLength;
	}

	public void setStreamFieldLength(Range streamFieldLength) {
		this.streamFieldLength = streamFieldLength;
	}

	public Range getStringLength() {
		return stringLength;
	}

	public void setStringLength(Range stringLength) {
		this.stringLength = stringLength;
	}

	public Range getTimeseriesSampleCount() {
		return timeseriesSampleCount;
	}

	public void setTimeseriesSampleCount(Range timeseriesSampleCount) {
		this.timeseriesSampleCount = timeseriesSampleCount;
	}

	public Instant getTimeseriesStartTime() {
		return timeseriesStartTime;
	}

	public void setTimeseriesStartTime(Instant timeseriesStartTime) {
		this.timeseriesStartTime = timeseriesStartTime;
	}

	public Range getZsetMemberCount() {
		return zsetMemberCount;
	}

	public void setZsetMemberCount(Range zsetMemberCount) {
		this.zsetMemberCount = zsetMemberCount;
	}

	public Range getZsetMemberLength() {
		return zsetMemberLength;
	}

	public void setZsetMemberLength(Range zsetMemberLength) {
		this.zsetMemberLength = zsetMemberLength;
	}

	public Range getZsetScore() {
		return zsetScore;
	}

	public void setZsetScore(Range zsetScore) {
		this.zsetScore = zsetScore;
	}

	public String getIndex() {
		return index;
	}

	public void setIndex(String index) {
		this.index = index;
	}

	public String getKeySepataror() {
		return keySepataror;
	}

	public void setKeySepataror(String keySepataror) {
		this.keySepataror = keySepataror;
	}

}
