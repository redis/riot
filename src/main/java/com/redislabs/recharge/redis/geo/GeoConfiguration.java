package com.redislabs.recharge.redis.geo;

import com.redislabs.recharge.redis.CollectionRedisConfiguration;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class GeoConfiguration extends CollectionRedisConfiguration {
	private String longitude;
	private String latitude;
}
