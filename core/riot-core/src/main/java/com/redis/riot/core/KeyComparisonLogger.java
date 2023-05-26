package com.redis.riot.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.reader.KeyComparison;

import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;

@SuppressWarnings("unchecked")
public class KeyComparisonLogger {

	/**
	 * Represents a failed index search.
	 * 
	 */
	public static final int INDEX_NOT_FOUND = -1;

	private final Logger log;

	public KeyComparisonLogger() {
		this(Logger.getLogger(KeyComparisonLogger.class.getName()));
	}

	public KeyComparisonLogger(Logger logger) {
		this.log = logger;
	}

	public void log(KeyComparison comparison) {
		switch (comparison.getStatus()) {
		case MISSING:
			log.log(Level.WARNING, "Missing key {0}", comparison.getSource().getKey());
			break;
		case TTL:
			log.log(Level.WARNING, "TTL mismatch for key {0}: {1} <> {2}",
					new Object[] { comparison.getSource().getKey(), comparison.getSource().getTtl(),
							comparison.getTarget().getTtl() });
			break;
		case TYPE:
			log.log(Level.WARNING, "Type mismatch for key {0}: {1} <> {2}",
					new Object[] { comparison.getSource().getKey(), comparison.getSource().getType(),
							comparison.getTarget().getType() });
			break;
		case VALUE:
			switch (comparison.getSource().getType()) {
			case DataStructure.SET:
				showSetDiff(comparison);
				break;
			case DataStructure.LIST:
				showListDiff(comparison);
				break;
			case DataStructure.ZSET:
				showSortedSetDiff(comparison);
				break;
			case DataStructure.STREAM:
				showStreamDiff(comparison);
				break;
			case DataStructure.STRING:
			case DataStructure.JSON:
				showStringDiff(comparison);
				break;
			case DataStructure.HASH:
				showHashDiff(comparison);
				break;
			case DataStructure.TIMESERIES:
				showListDiff(comparison);
				break;
			default:
				log.log(Level.WARNING, "Value mismatch for key '{}'", comparison.getSource().getKey());
				break;
			}
			break;
		case OK:
			break;
		}
	}

	private void showHashDiff(KeyComparison comparison) {
		Map<String, String> sourceHash = (Map<String, String>) comparison.getSource().getValue();
		Map<String, String> targetHash = (Map<String, String>) comparison.getTarget().getValue();
		Map<String, String> diff = new HashMap<>();
		diff.putAll(sourceHash);
		diff.putAll(targetHash);
		diff.entrySet()
				.removeAll(sourceHash.size() <= targetHash.size() ? sourceHash.entrySet() : targetHash.entrySet());
		log.log(Level.WARNING, "Value mismatch for hash {0} on fields: {1}",
				new Object[] { comparison.getSource().getKey(), diff.keySet() });
	}

	private void showStringDiff(KeyComparison comparison) {
		String sourceString = (String) comparison.getSource().getValue();
		String targetString = (String) comparison.getTarget().getValue();
		int diffIndex = indexOfDifference(sourceString, targetString);
		log.log(Level.WARNING, "Value mismatch for string {0} at offset {1}",
				new Object[] { comparison.getSource().getKey(), diffIndex });
	}

	/**
	 * <p>
	 * Compares two CharSequences, and returns the index at which the CharSequences
	 * begin to differ.
	 * </p>
	 *
	 * <p>
	 * For example, {@code indexOfDifference("i am a machine", "i am a robot") -> 7}
	 * </p>
	 *
	 * <pre>
	 * StringUtils.indexOfDifference(null, null) = -1
	 * StringUtils.indexOfDifference("", "") = -1
	 * StringUtils.indexOfDifference("", "abc") = 0
	 * StringUtils.indexOfDifference("abc", "") = 0
	 * StringUtils.indexOfDifference("abc", "abc") = -1
	 * StringUtils.indexOfDifference("ab", "abxyz") = 2
	 * StringUtils.indexOfDifference("abcde", "abxyz") = 2
	 * StringUtils.indexOfDifference("abcde", "xyz") = 0
	 * </pre>
	 *
	 * @param cs1 the first CharSequence, may be null
	 * @param cs2 the second CharSequence, may be null
	 * @return the index where cs1 and cs2 begin to differ; -1 if they are equal
	 * @since 2.0
	 * @since 3.0 Changed signature from indexOfDifference(String, String) to
	 *        indexOfDifference(CharSequence, CharSequence)
	 */
	private static int indexOfDifference(final CharSequence cs1, final CharSequence cs2) {
		if (cs1 == cs2) {
			return INDEX_NOT_FOUND;
		}
		if (cs1 == null || cs2 == null) {
			return 0;
		}
		int i;
		for (i = 0; i < cs1.length() && i < cs2.length(); ++i) {
			if (cs1.charAt(i) != cs2.charAt(i)) {
				break;
			}
		}
		if (i < cs2.length() || i < cs1.length()) {
			return i;
		}
		return INDEX_NOT_FOUND;
	}

	private void showListDiff(KeyComparison comparison) {
		List<?> sourceList = (List<?>) comparison.getSource().getValue();
		List<?> targetList = (List<?>) comparison.getTarget().getValue();
		if (sourceList.size() != targetList.size()) {
			log.log(Level.WARNING, "Size mismatch for {0} {1}: {2} <> {3}",
					new Object[] { comparison.getSource().getType(), comparison.getSource().getKey(), sourceList.size(),
							targetList.size() });
			return;
		}
		List<Integer> diff = new ArrayList<>();
		for (int index = 0; index < sourceList.size(); index++) {
			if (!sourceList.get(index).equals(targetList.get(index))) {
				diff.add(index);
			}
		}
		log.log(Level.WARNING, "Value mismatch for {0} {1} at indexes {2}",
				new Object[] { comparison.getSource().getType(), comparison.getSource().getKey(), diff });
	}

	private void showSetDiff(KeyComparison comparison) {
		Set<String> sourceSet = (Set<String>) comparison.getSource().getValue();
		Set<String> targetSet = (Set<String>) comparison.getTarget().getValue();
		Set<String> missing = new HashSet<>(sourceSet);
		missing.removeAll(targetSet);
		Set<String> extra = new HashSet<>(targetSet);
		extra.removeAll(sourceSet);
		log.log(Level.WARNING, "Value mismatch for set {0}: {1} <> {2}",
				new Object[] { comparison.getSource().getKey(), missing, extra });
	}

	private void showSortedSetDiff(KeyComparison comparison) {
		List<ScoredValue<String>> sourceList = (List<ScoredValue<String>>) comparison.getSource().getValue();
		List<ScoredValue<String>> targetList = (List<ScoredValue<String>>) comparison.getTarget().getValue();
		List<ScoredValue<String>> missing = new ArrayList<>(sourceList);
		missing.removeAll(targetList);
		List<ScoredValue<String>> extra = new ArrayList<>(targetList);
		extra.removeAll(sourceList);
		log.log(Level.WARNING, "Value mismatch for sorted set {0}: {1} <> {2}",
				new Object[] { comparison.getSource().getKey(), print(missing), print(extra) });
	}

	private List<String> print(List<ScoredValue<String>> list) {
		return list.stream().map(v -> v.getValue() + "@" + v.getScore()).collect(Collectors.toList());
	}

	private void showStreamDiff(KeyComparison comparison) {
		List<StreamMessage<String, String>> sourceMessages = (List<StreamMessage<String, String>>) comparison
				.getSource().getValue();
		List<StreamMessage<String, String>> targetMessages = (List<StreamMessage<String, String>>) comparison
				.getTarget().getValue();
		List<StreamMessage<String, String>> missing = new ArrayList<>(sourceMessages);
		missing.removeAll(targetMessages);
		List<StreamMessage<String, String>> extra = new ArrayList<>(targetMessages);
		extra.removeAll(sourceMessages);
		log.log(Level.WARNING, "Value mismatch for stream {0}: {1} <> {2}",
				new Object[] { comparison.getSource().getKey(),
						missing.stream().map(StreamMessage::getId).collect(Collectors.toList()),
						extra.stream().map(StreamMessage::getId).collect(Collectors.toList()) });
	}

}