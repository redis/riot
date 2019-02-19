package com.redislabs.recharge.redis;

import com.redislabs.lettusearch.search.field.Matcher;

import lombok.Data;

@Data
public class RediSearchField {
	private String name;
	private RediSearchFieldType type = RediSearchFieldType.Text;
	private boolean sortable;
	private boolean noIndex;
	private Double weight;
	private boolean noStem;
	private Matcher matcher;
}
