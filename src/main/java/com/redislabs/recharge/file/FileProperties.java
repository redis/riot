package com.redislabs.recharge.file;

import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.boot.context.properties.ConfigurationProperties;

import lombok.Data;

@Data
@ConfigurationProperties(prefix = "file")
public class FileProperties {

	private String path;
	private FileType type;
	private Boolean gzip;
	private String delimiter;
	private int[] includedFields;
	private Character quoteCharacter;
	private String[] fields;
	private String encoding = FlatFileItemReader.DEFAULT_CHARSET;
	private boolean header = true;
	private int linesToSkip = 0;
	private String[] ranges = new String[0];
	private boolean strict;
	
	public static enum FileType {
		Csv, Fw, Json
	}

}