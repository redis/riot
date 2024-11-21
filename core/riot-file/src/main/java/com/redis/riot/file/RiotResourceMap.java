package com.redis.riot.file;

import java.io.IOException;
import java.net.FileNameMap;
import java.net.URLConnection;
import java.nio.file.Files;
import java.util.LinkedHashSet;
import java.util.Set;

import org.springframework.core.io.Resource;

public class RiotResourceMap implements ResourceMap {

	private final Set<FileNameMap> fileNameMaps = defaultFileNameMaps();

	public void addFileNameMap(FileNameMap map) {
		fileNameMaps.add(map);
	}

	public static Set<FileNameMap> defaultFileNameMaps() {
		Set<FileNameMap> maps = new LinkedHashSet<>();
		maps.add(new JsonLinesFileNameMap());
		return maps;
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
		String normalizedFilename = FileUtils.stripGzipSuffix(filename);
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

	private static class JsonLinesFileNameMap implements FileNameMap {

		public static final String JSONL_SUFFIX = ".jsonl";

		@Override
		public String getContentTypeFor(String fileName) {
			if (fileName == null) {
				return null;
			}
			if (fileName.endsWith(JSONL_SUFFIX)) {
				return FileUtils.JSON_LINES.toString();
			}
			return null;
		}

	}

}
