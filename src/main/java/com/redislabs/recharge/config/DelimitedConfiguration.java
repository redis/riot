package com.redislabs.recharge.config;

import lombok.Data;

@Data
public class DelimitedConfiguration {

	String delimiter;
	Integer[] includedFields;
	Character quoteCharacter;

}