package com.redislabs.recharge.redis;

import lombok.Data;

@Data
public class RedisWriterConfiguration {
	private StringConfiguration string;
	private HashConfiguration hash;
	private ListConfiguration list;
	private SetConfiguration set;
	private ZSetConfiguration zset;
	private GeoConfiguration geo;
	private SearchConfiguration search;
	private SuggestConfiguration suggest;
	private StreamConfiguration stream;
}
