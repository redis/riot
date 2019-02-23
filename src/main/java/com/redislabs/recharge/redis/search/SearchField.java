package com.redislabs.recharge.redis.search;

import com.redislabs.lettusearch.search.field.GeoField;
import com.redislabs.lettusearch.search.field.NumericField;
import com.redislabs.lettusearch.search.field.TagField;
import com.redislabs.lettusearch.search.field.TextField;

import lombok.Data;

@Data
public class SearchField {

	private NumericField numeric;
	private TextField text;
	private TagField tag;
	private GeoField geo;

}
