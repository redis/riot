package com.redislabs.recharge.config;

import lombok.Data;

@Data
public class KeyConfiguration {

	String prefix;
	String separator = ":";
	String[] fields;

}