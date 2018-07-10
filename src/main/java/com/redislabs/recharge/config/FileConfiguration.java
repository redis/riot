package com.redislabs.recharge.config;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.util.ResourceUtils;

import lombok.Data;

@Data
public class FileConfiguration {

	private Pattern filePathPattern = Pattern.compile("(?<basename>.+)\\.(?<extension>\\w+)(?<gz>\\.gz)?");

	String file;
	Boolean gzip;
	String encoding;
	FlatFileConfiguration flat;

	public Boolean getGzip() {
		if (gzip == null) {
			String gz = getRegexGroupFromFilename("gz");
			return gz != null && gz.length() > 0;
		}
		return gzip;
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