package com.redislabs.recharge.config;

import lombok.Data;

@Data
public class FlatFileConfiguration {

	Integer linesToSkip;
	String[] fields;
	DelimitedConfiguration delimited;
	FixedLengthConfiguration fixedLength;

}