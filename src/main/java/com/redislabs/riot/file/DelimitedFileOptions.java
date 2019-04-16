package com.redislabs.riot.file;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class DelimitedFileOptions extends FlatFileOptions {

	private boolean header;
	private String delimiter;
	private int[] includedFields;
	private Character quoteCharacter;

}
