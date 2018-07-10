package com.redislabs.recharge.config;

import lombok.Data;

@Data
public class ZSetConfiguration {

	String score;
	KeyConfiguration key = new KeyConfiguration();

}