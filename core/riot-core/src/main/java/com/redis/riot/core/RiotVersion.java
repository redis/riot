package com.redis.riot.core;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.ResourceBundle;

public class RiotVersion {

	private static final String JVM_FORMAT = "%s (%s %s)";
	private static final String SEPARATOR = "------------------------------------------------------------%n";
	private static final ResourceBundle BUNDLE = ResourceBundle.getBundle(RiotVersion.class.getName());
	private static final String RIOT_VERSION = BUNDLE.getString("riot_version");
	private static final String BUILD_DATE = BUNDLE.getString("build_date");
	private static final String BUILD_TIME = BUNDLE.getString("build_time");
	private static final String BUILD_REVISION = BUNDLE.getString("build_revision");
	private static final String RIOT_FORMAT = "riot %s%n";
	private static final String RIOT_VERSION_FORMAT = "RIOT%s";

	private RiotVersion() {
		// noop
	}

	public static String getPlainVersion() {
		return RIOT_VERSION;
	}

	public static String riotVersion() {
		return String.format(RIOT_VERSION_FORMAT, RIOT_VERSION);
	}

	public static void banner(PrintStream out) {
		banner(out, true);
	}

	public static void banner(PrintStream out, boolean full) {
		banner(RiotUtils.newPrintWriter(out), bannerOptions(full));
	}

	public static void banner(PrintWriter out) {
		banner(out, bannerOptions(true));
	}

	private static BannerOptions bannerOptions(boolean full) {
		return bannerOptions().full(full);
	}

	private static BannerOptions bannerOptions() {
		return new BannerOptions().buildDate(BUILD_DATE).buildRevision(BUILD_REVISION).buildTime(BUILD_TIME)
				.format(RIOT_FORMAT).version(RIOT_VERSION);
	}

	public static class BannerOptions {

		private boolean full;
		private String format;
		private String version;
		private String buildDate;
		private String buildTime;
		private String buildRevision;

		public boolean isFull() {
			return full;
		}

		public BannerOptions full(boolean full) {
			this.full = full;
			return this;
		}

		public String getFormat() {
			return format;
		}

		public BannerOptions format(String format) {
			this.format = format;
			return this;
		}

		public String getVersion() {
			return version;
		}

		public BannerOptions version(String version) {
			this.version = version;
			return this;
		}

		public String getBuildDate() {
			return buildDate;
		}

		public BannerOptions buildDate(String date) {
			this.buildDate = date;
			return this;
		}

		public String getBuildTime() {
			return buildTime;
		}

		public BannerOptions buildTime(String time) {
			this.buildTime = time;
			return this;
		}

		public String getBuildRevision() {
			return buildRevision;
		}

		public BannerOptions buildRevision(String revision) {
			this.buildRevision = revision;
			return this;
		}

	}

	public static void banner(PrintWriter out, BannerOptions options) {
		if (options.full) {
			out.printf(SEPARATOR);
			out.printf(options.format, options.version);
			out.printf(SEPARATOR);
			out.printf("Build time:   %s %s%n", options.buildDate, options.buildTime);
			out.printf("Revision:     %s%n", options.buildRevision);
			out.printf("JVM:          %s%n", jvm());
			out.printf(SEPARATOR);
		} else {
			out.printf(options.format, options.version);
		}
	}

	private static String jvm() {
		return String.format(JVM_FORMAT, System.getProperty("java.version"), System.getProperty("java.vendor"),
				System.getProperty("java.vm.version"));
	}

}