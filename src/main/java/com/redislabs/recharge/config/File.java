package com.redislabs.recharge.config;

public class File {

	private String path;
	private FileType type;
	private Boolean gzip;
	private String encoding;
	private Xml xml = new Xml();
	private FlatFile flat = new FlatFile();

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public FlatFile getFlat() {
		return flat;
	}

	public void setFlat(FlatFile flat) {
		this.flat = flat;
	}

	public String getEncoding() {
		return encoding;
	}

	public void setEncoding(String encoding) {
		this.encoding = encoding;
	}

	public Boolean getGzip() {
		return gzip;
	}

	public void setGzip(Boolean gzip) {
		this.gzip = gzip;
	}

	public FileType getType() {
		return type;
	}

	public void setType(FileType type) {
		this.type = type;
	}

	public Xml getXml() {
		return xml;
	}

	public void setXml(Xml xml) {
		this.xml = xml;
	}

	public boolean isEnabled() {
		return path != null && path.length() > 0;
	}

}