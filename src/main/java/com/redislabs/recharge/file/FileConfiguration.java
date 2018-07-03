package com.redislabs.recharge.file;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.util.ResourceUtils;

@Configuration
@ConfigurationProperties(prefix = "")
@EnableAutoConfiguration
public class FileConfiguration {

	private Pattern filePathPattern = Pattern.compile("(?<basename>.+)\\.(?<extension>\\w+)(?<gz>\\.gz)?");

	private String file;
	private Boolean gzip;
	private String encoding;

	public String getFile() {
		return file;
	}

	public void setFile(String path) {
		this.file = path;
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

	public boolean isEnabled() {
		return file != null && file.length() > 0;
	}

	private String getRegexGroupFromFilename(String groupName) {
		Matcher matcher = filePathPattern.matcher(new java.io.File(file).getName());
		if (matcher.find()) {
			return matcher.group(groupName);
		}
		return null;
	}

	public Resource getResource() throws IOException {
		Resource resource = getResource(file);
		if (Boolean.TRUE.equals(getGzip())) {
			return getGZipResource(resource);
		}
		return resource;
	}

	private Resource getGZipResource(Resource resource) throws IOException {
		return new InputStreamResource(new GZIPInputStream(resource.getInputStream()));
	}

	private Resource getResource(String path) throws MalformedURLException {
		if (ResourceUtils.isUrl(path)) {
			return new UrlResource(path);
		}
		return new FileSystemResource(path);
	}

}