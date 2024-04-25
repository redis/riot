package com.redis.riot.core;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.ResourceBundle;

public class RiotVersion {

	private static final ResourceBundle BUNDLE = ResourceBundle.getBundle(RiotVersion.class.getName());
	private static final String RIOT_VERSION = BUNDLE.getString("riot_version");
	private static final String BUILD_DATE = BUNDLE.getString("build_date");
	private static final String BUILD_TIME = BUNDLE.getString("build_time");
	private static final String BUILD_REVISION = BUNDLE.getString("build_revision");
	private static final String SEPARATOR = "------------------------------------------------------------%n";
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
		banner(RiotUtils.newPrintWriter(out), full);
	}

	public static void banner(PrintWriter out) {
		banner(out, true);
	}

	public static void banner(PrintWriter out, boolean full) {
		if (full) {
			out.printf(SEPARATOR);
			out.printf(RIOT_FORMAT, RIOT_VERSION);

			String jvm = System.getProperty("java.version") + " (" + System.getProperty("java.vendor") + " "
					+ System.getProperty("java.vm.version") + ")";

			out.printf(SEPARATOR);
			out.printf("Build time:   %s %s%n", BUILD_DATE, BUILD_TIME);
			out.printf("Revision:     %s%n", BUILD_REVISION);
			out.printf("JVM:          %s%n", jvm);
			out.printf(SEPARATOR);
		} else {
			out.printf(RIOT_FORMAT, RIOT_VERSION);
		}
	}
}