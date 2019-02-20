package com.redislabs.recharge.redis.search;

import com.redislabs.lettusearch.search.field.Matcher;

import lombok.Data;

@Data
public class SearchField {
	private String name;
	private FieldType type = FieldType.Text;
	private boolean sortable;
	private boolean noIndex;
	private Double weight;
	private boolean noStem;
	private Matcher matcher;
	private String separator;

	public static enum FieldType {
		Text, Numeric, Geo, Tag
	}
}
