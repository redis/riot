package com.redislabs.recharge.file;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "file")
@EnableAutoConfiguration
public class FileConfiguration {

	private Pattern filePathPattern = Pattern.compile("(?<basename>.+)\\.(?<extension>\\w+)(?<gz>\\.gz)?");

	private String path;
	private FileType type;
	private Boolean gzip;
	private String encoding;
	private FlatFileConfiguration flat = new FlatFileConfiguration();

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public FlatFileConfiguration getFlat() {
		return flat;
	}

	public void setFlat(FlatFileConfiguration flat) {
		this.flat = flat;
	}

	public String getEncoding() {
		return encoding;
	}

	public void setEncoding(String encoding) {
		this.encoding = encoding;
	}

	public Boolean getGzip() {
		if (gzip == null) {
			String gz = getRegexGroupFromFilename("gz");
			return gz != null && gz.length() > 0;
		}
		return gzip;
	}

	public void setGzip(Boolean gzip) {
		this.gzip = gzip;
	}

	public FileType getType() {
		if (type == null) {
			return getTypeFromExtension(getRegexGroupFromFilename("extension"));
		}
		return type;
	}

	private FileType getTypeFromExtension(String extension) {
		switch (extension) {
		case "fw":
			return FileType.FixedLength;
		case "csv":
			return FileType.Delimited;
		case "json":
			return FileType.JSON;
		case "xml":
			return FileType.XML;
		default:
			return null;
		}
	}

	public void setType(FileType type) {
		this.type = type;
	}

	public boolean isEnabled() {
		return path != null && path.length() > 0;
	}

	private String getRegexGroupFromFilename(String groupName) {
		Matcher matcher = filePathPattern.matcher(new java.io.File(path).getName());
		if (matcher.find()) {
			return matcher.group(groupName);
		}
		return null;
	}

}