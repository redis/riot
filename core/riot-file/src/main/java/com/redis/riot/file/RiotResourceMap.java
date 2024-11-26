package com.redis.riot.file;

import java.io.IOException;
import java.net.FileNameMap;
import java.net.URLConnection;
import java.nio.file.Files;
import java.util.LinkedHashSet;
import java.util.Set;

import org.springframework.core.io.Resource;
import org.springframework.util.MimeType;

public class RiotResourceMap implements ResourceMap {

	private final Set<FileNameMap> fileNameMaps = new LinkedHashSet<>();

	public void addFileNameMap(FileNameMap map) {
		fileNameMaps.add(map);
	}

	@Override
	public MimeType getContentTypeFor(Resource resource) {
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
		return MimeType.valueOf(type);
	}

	public MimeType getContentTypeFor(String filename) {
		String normalizedFilename = ResourceFactory.stripGzipSuffix(filename);
		String type = URLConnection.guessContentTypeFromName(normalizedFilename);
		if (type != null) {
			return MimeType.valueOf(type);
		}
		for (FileNameMap nameMap : fileNameMaps) {
			String mapType = nameMap.getContentTypeFor(normalizedFilename);
			if (mapType != null) {
				return MimeType.valueOf(mapType);
			}
		}
		throw new IllegalArgumentException("Could not determine type of " + filename);
	}

	public static RiotResourceMap defaultResourceMap() {
		RiotResourceMap resourceMap = new RiotResourceMap();
		resourceMap.addFileNameMap(new JsonLinesFileNameMap());
		return resourceMap;
	}

	private static class JsonLinesFileNameMap implements FileNameMap {

		public static final String JSONL_SUFFIX = ".jsonl";

		@Override
		public String getContentTypeFor(String fileName) {
			if (fileName == null) {
				return null;
			}
			if (fileName.endsWith(JSONL_SUFFIX)) {
				return JSON_LINES.toString();
			}
			return null;
		}

	}
}
