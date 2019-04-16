package com.redislabs.riot.redis.writer.search.aggregate;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class PropertyReducer extends Reducer {

	private String property;

}
