package com.redislabs.recharge.redis.search;

import lombok.Data;

@Data
public class SearchField {

	private Field numeric;
	private Text text;
	private Field tag;
	private Field geo;

}
