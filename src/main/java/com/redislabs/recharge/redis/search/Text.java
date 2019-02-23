package com.redislabs.recharge.redis.search;

import com.redislabs.lettusearch.search.field.PhoneticMatcher;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class Text extends Field {

	private Double weight;
	private boolean noStem;
	private PhoneticMatcher matcher;

}
