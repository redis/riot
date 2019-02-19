package com.redislabs.recharge.file;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class DelimitedFileConfiguration extends FlatFileConfiguration {
	private String delimiter;
	private Integer[] includedFields;
	private Character quoteCharacter;
}
