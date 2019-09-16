package com.redislabs.riot.cli.file;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;

import org.springframework.core.io.UrlResource;

public class UncustomizedUrlResource extends UrlResource {

	public UncustomizedUrlResource(String path) throws MalformedURLException {
		super(path);
	}

	public UncustomizedUrlResource(String protocol, String location, String fragment) throws MalformedURLException {
		super(protocol, location, fragment);
	}

	public UncustomizedUrlResource(String protocol, String location) throws MalformedURLException {
		super(protocol, location);
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
