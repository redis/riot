package com.redislabs.recharge.file;

import java.util.LinkedHashMap;
import java.util.Map;

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

	@SuppressWarnings("serial")
	private Map<String, FileType> fileTypes = new LinkedHashMap<String, FileType>() {
		{
			put("dat", FileType.Delimited);
			put("csv", FileType.Delimited);
			put("txt", FileType.FixedLength);
		}
	};

}
