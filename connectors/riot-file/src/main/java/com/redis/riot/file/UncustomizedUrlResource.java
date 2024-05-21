package com.redis.riot.file;

import org.springframework.core.io.UrlResource;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;

public class UncustomizedUrlResource extends UrlResource {

	public UncustomizedUrlResource(String path) throws MalformedURLException {
		super(path);
	}

	public UncustomizedUrlResource(URI uri) throws MalformedURLException {
		super(uri);
	}

	public UncustomizedUrlResource(URL url) {
		super(url);
	}

	@Override
	protected void customizeConnection(HttpURLConnection con) throws IOException {
		// do nothing
	}

}
