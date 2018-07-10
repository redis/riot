package com.redislabs.recharge.config;

import lombok.Data;

@Data
public class GeoConfiguration {

	String xField;
	String yField;
	KeyConfiguration key = new KeyConfiguration();

}
