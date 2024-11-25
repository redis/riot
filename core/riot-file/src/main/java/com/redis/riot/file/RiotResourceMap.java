package com.redis.riot.file;

import java.io.IOException;
import java.net.FileNameMap;
import java.net.URLConnection;
import java.nio.file.Files;
import java.util.LinkedHashSet;
import java.util.Set;

import org.springframework.core.io.Resource;

public class RiotResourceMap implements ResourceMap {

	private final Set<FileNameMap> fileNameMaps = new LinkedHashSet<>();

	public void addFileNameMap(FileNameMap map) {
		fileNameMaps.add(map);
	}

	@Override
	public String getContentTypeFor(Resource resource) {
		String type = null;
		if (resource.isFile()) {
			try {
				type = Files.probeContentType(resource.getFile().toPath());
			} catch (IOException e) {
				// ignore
			}
		}
		if (type == null) {
			return getContentTypeFor(resource.getFilename());
		}
		return type;
	}

	public String getContentTypeFor(String filename) {
		String normalizedFilename = ResourceFactory.stripGzipSuffix(filename);
		String type = URLConnection.guessContentTypeFromName(normalizedFilename);
		if (type != null) {
			return type;
		}
		for (FileNameMap nameMap : fileNameMaps) {
			String mapType = nameMap.getContentTypeFor(normalizedFilename);
			if (mapType != null) {
				return mapType;
			}
		}
		throw new IllegalArgumentException("Could not determine type of " + filename);
	}

}
