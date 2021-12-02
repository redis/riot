package com.redis.riot.file.resource;

import org.springframework.core.io.UrlResource;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;

public class UncustomizedUrlResource extends UrlResource {

	public UncustomizedUrlResource(String path) throws MalformedURLException {
		super(path);
	}

	@Override
	protected void customizeConnection(HttpURLConnection con) throws IOException {
		// do nothing
	}

}
