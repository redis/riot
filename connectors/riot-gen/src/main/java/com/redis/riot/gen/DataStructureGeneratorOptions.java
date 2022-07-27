package com.redis.riot.gen;

import static com.redis.spring.batch.reader.RandomDataStructureItemReader.DEFAULT_COLLECTION_CARDINALITY;
import static com.redis.spring.batch.reader.RandomDataStructureItemReader.DEFAULT_KEYSPACE;
import static com.redis.spring.batch.reader.RandomDataStructureItemReader.DEFAULT_STRING_VALUE_SIZE;
import static com.redis.spring.batch.reader.RandomDataStructureItemReader.DEFAULT_ZSET_SCORE;

import java.util.OptionalInt;

import com.redis.spring.batch.DataStructure.Type;
import com.redis.spring.batch.reader.RandomDataStructureItemReader;

import picocli.CommandLine.Option;

public class DataStructureGeneratorOptions extends GeneratorOptions {

	@Option(names = "--keyspace", description = "Keyspace prefix for generated data structures (default: ${DEFAULT-VALUE}).", paramLabel = "<str>")
	private String keyspace = DEFAULT_KEYSPACE;
	@Option(arity = "1..*", names = "--type", description = "Data structure types to generate: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<type>")
	private Type[] types = RandomDataStructureItemReader.defaultTypes().toArray(Type[]::new);
	@Option(names = "--min-expiration", description = "Minimum TTL in seconds.", paramLabel = "<secs>")
	private OptionalInt minExpiration = OptionalInt.empty();
	@Option(names = "--max-expiration", description = "Maximum TTL in seconds.", paramLabel = "<secs>")
	private OptionalInt maxExpiration = OptionalInt.empty();
	@Option(names = "--min-cardinality", description = "Minimum number of elements in collection data structures (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int minCollectionCardinality = DEFAULT_COLLECTION_CARDINALITY.getMinimum();
	@Option(names = "--max-cardinality", description = "Maximum number of elements in collection data structures (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int maxCollectionCardinality = DEFAULT_COLLECTION_CARDINALITY.getMaximum();
	@Option(names = "--min-string-size", description = "Minimum length for strings (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int minStringSize = DEFAULT_STRING_VALUE_SIZE.getMinimum();
	@Option(names = "--max-string-size", description = "Maximum length for strings (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int maxStringSize = DEFAULT_STRING_VALUE_SIZE.getMaximum();
	@Option(names = "--min-zset-score", description = "Minimum score for sorted sets (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private double minZsetScore = DEFAULT_ZSET_SCORE.getMinimum();
	@Option(names = "--max-zset-score", description = "Maximum score for sorted sets (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private double maxZsetScore = DEFAULT_ZSET_SCORE.getMaximum();

	public String getKeyspace() {
		return keyspace;
	}

	public void setKeyspace(String keyspace) {
		this.keyspace = keyspace;
	}

	public Type[] getTypes() {
		return types;
	}

	public void setTypes(Type... types) {
		this.types = types;
	}

	public OptionalInt getMinExpiration() {
		return minExpiration;
	}

	public void setMinExpiration(OptionalInt minExpiration) {
		this.minExpiration = minExpiration;
	}

	public OptionalInt getMaxExpiration() {
		return maxExpiration;
	}

	public void setMaxExpiration(OptionalInt maxExpiration) {
		this.maxExpiration = maxExpiration;
	}

	public int getMinCollectionCardinality() {
		return minCollectionCardinality;
	}

	public void setMinCollectionCardinality(int minCollectionCardinality) {
		this.minCollectionCardinality = minCollectionCardinality;
	}

	public int getMaxCollectionCardinality() {
		return maxCollectionCardinality;
	}

	public void setMaxCollectionCardinality(int maxCollectionCardinality) {
		this.maxCollectionCardinality = maxCollectionCardinality;
	}

	public int getMinStringSize() {
		return minStringSize;
	}

	public void setMinStringSize(int minStringSize) {
		this.minStringSize = minStringSize;
	}

	public int getMaxStringSize() {
		return maxStringSize;
	}

	public void setMaxStringSize(int maxStringSize) {
		this.maxStringSize = maxStringSize;
	}

	public double getMinZsetScore() {
		return minZsetScore;
	}

	public void setMinZsetScore(double minZsetScore) {
		this.minZsetScore = minZsetScore;
	}

	public double getMaxZsetScore() {
		return maxZsetScore;
	}

	public void setMaxZsetScore(double maxZsetScore) {
		this.maxZsetScore = maxZsetScore;
	}

}
