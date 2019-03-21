package com.redislabs.recharge.redis.search.search;

import com.redislabs.lettusearch.search.SortBy.Direction;
import com.redislabs.recharge.redis.search.RediSearchCommandConfiguration;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class SearchConfiguration extends RediSearchCommandConfiguration {

	private String query;
	private boolean verbatim;
	private boolean noContent;
	private boolean noStopWords;
	private SortByConfiguration sortBy;
	private String language;
	private LimitConfiguration limit;
	private boolean withPayloads;
	private boolean withScores;

	@Data
	public static class LimitConfiguration {
		private long num;
		private long offset;
	}

	@Data
	public static class SortByConfiguration {
		private String field;
		private Direction direction;
	}

}
