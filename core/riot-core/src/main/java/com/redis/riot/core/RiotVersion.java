package com.redis.riot.core;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.ResourceBundle;

public class RiotVersion {

	private static final ResourceBundle BUNDLE = ResourceBundle.getBundle(RiotVersion.class.getName());
	private static final String RIOT_VERSION = BUNDLE.getString("riot_version");
	private static final String SEPARATOR = "------------------------------------------------------------%n";
	private static final String RIOT_FORMAT = "riot %s%n";

	private RiotVersion() {
		// noop
	}

	public static String getVersion() {
		return RIOT_VERSION;
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
		banner(out, full, BUNDLE, RIOT_FORMAT, RIOT_VERSION);
	}

	public static void banner(PrintWriter out, boolean full, ResourceBundle bundle, String format, String version) {
		if (full) {
			out.printf(SEPARATOR);
			out.printf(format, version);
			out.printf(SEPARATOR);
			out.printf("Build time:   %s %s%n", bundle.getString("build_date"), bundle.getString("build_time"));
			out.printf("Revision:     %s%n", bundle.getString("build_revision"));
			out.printf("JVM:          %s (%s %s)%n", System.getProperty("java.version"),
					System.getProperty("java.vendor"), System.getProperty("java.vm.version"));
			out.printf(SEPARATOR);
		} else {
			out.printf(format, version);
		}
	}

}