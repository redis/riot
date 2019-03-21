package com.redislabs.recharge.file;

import org.springframework.context.annotation.Configuration;

import lombok.Data;

@Configuration
@Data
public class FileReaderConfiguration {

	private String path;
	private Boolean gzip;
	private FileType type;
	private DelimitedFileConfiguration delimited = new DelimitedFileConfiguration();
	private FixedLengthFileConfiguration fixedLength = new FixedLengthFileConfiguration();
	private JsonFileConfiguration json = new JsonFileConfiguration();

}
