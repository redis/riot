package com.redislabs.recharge.file;

import lombok.Data;

@Data
public class FileConfiguration {

	private String path;
	private Boolean gzip;
	private FileType type;

}
