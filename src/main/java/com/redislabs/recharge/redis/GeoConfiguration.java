package com.redislabs.recharge.redis;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class GeoConfiguration extends CollectionRedisWriterConfiguration {
	private String longitude;
	private String latitude;
}
