package com.redislabs.recharge.redisearch;

import java.util.ArrayList;
import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

import com.redislabs.lettusearch.search.SortBy.Direction;

import lombok.Data;

@Data
@ConfigurationProperties(prefix = "")
public class RediSearchProperties {

	private RediSearchType type = RediSearchType.Search;
	private String index;
	private boolean drop = false;
	private boolean dropKeepDocs = false;
	private boolean create = true;
	private String[] keys = new String[0];
	private List<SchemaField> schema = new ArrayList<>();
	private String language;
	private String score;
	private String payload;
	private double defaultScore = 1d;
	private boolean replace;
	private boolean replacePartial;
	private boolean noSave;
	private String query;
	private boolean verbatim;
	private boolean noContent;
	private boolean noStopWords;
	private SortByConfiguration sortBy;
	private LimitConfiguration limit;
	private boolean withPayloads;
	private boolean withScores;
	private boolean withSchema;
	private List<String> loads = new ArrayList<>();
	private List<AggregateOperation> operations = new ArrayList<>();
	private String field;
	private boolean increment;

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
