package com.redislabs.recharge.redis.search.add;

import com.redislabs.lettusearch.search.field.PhoneticMatcher;

import lombok.Data;

@Data
public class SchemaField {

	private String name;
	private Type type = Type.Text;
	private boolean sortable;
	private boolean noIndex;
	private Double weight;
	private boolean noStem;
	private PhoneticMatcher matcher;

	public static enum Type {
		Text, Numeric, Geo, Tag
	}

}
