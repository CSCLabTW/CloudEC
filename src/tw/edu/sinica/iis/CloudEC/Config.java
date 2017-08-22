/*
 * (C) Copyright 2017 The CloudEC Project and others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 * Contributors:
 *      Wei-Chun Chung (wcchung@iis.sinica.edu.tw)
 *      Chien-Chih Chen (rocky@iis.sinica.edu.tw)
 * 
 * CloudEC Project:
 *      https://github.com/CSCLabTW/CloudEC/
 */

package tw.edu.sinica.iis.CloudEC;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.mapred.JobConf;

public class Config {
	// working paths
	public static String hadoopReadPath = null;
	public static String hadoopBasePath = null;
	public static String hadoopTmpPath = null;
	public static String localBasePath = "work";

	// hadoop options
	public static int HADOOP_MAPPERS = 70;
	public static int HADOOP_REDUCERS = 70;
	public static long HADOOP_TIMEOUT = 0;
	public static String HADOOP_JAVAOPTS = "-Xmx950m";
	public static boolean HADOOP_MOCOMP = true;
	public static boolean HADOOP_OUTCOMP = true;

	// stage options
	public static String STARTSTAGE = null;
	public static String STOPSTAGE = null;
	public static String LARGEKMERFILTER = "off";
	public static String PINCHCORRECT = "on";
	public static String SPREADCORRECT = "on";
	public static String UNIQUEKMERFILTER = "on";

	// filter status
	public static boolean FILTER_P = false;
	public static boolean FILTER_S = true;

	// global parameters
	public static int K = 24;

	// scheme parameters
	public static String ARM_SCHEME = "";
	public static int ARM_L = -1;
	public static int ARM_H = 0;

	// stack status
	public static int STACK_MIN = 5;
	public static int STACK_MAX = -1;

	// stats
	public static String RUN_STATS = null;
	public static int PCRUN = 1;
	public static int SCRUN = 2;

	// output status
	public static String SHAVE_IGNORE = "on";
	public static String MERGE_IGNORE = "off";

	// map output compression codecs
	private class CODECS {
		private static final String SNAPPY = "org.apache.hadoop.io.compress.SnappyCodec";
		private static final String LZO = "com.hadoop.compression.lzo.LzoCodec";
	}

	public static void validateConfiguration() {
		int err = 0;

		if (RUN_STATS == null) {
			if (hadoopBasePath == null) {
				err++;
				System.err.println("ERROR: -out is required");
			}
			if (STARTSTAGE == null && hadoopReadPath == null) {
				err++;
				System.err.println("ERROR: -in is required");
			}
		}

		if (err > 0) {
			System.exit(1);
		}

		if (!hadoopBasePath.endsWith("/")) {
			hadoopBasePath += "/";
		}

		if (!localBasePath.endsWith("/")) {
			localBasePath += "/";
		}

		hadoopTmpPath = hadoopBasePath
				.substring(0, hadoopBasePath.length() - 1) + ".tmp" + "/";
	}

	public static void initializeConfiguration(JobConf conf) {
		validateConfiguration();

		conf.setNumMapTasks(HADOOP_MAPPERS);
		conf.setNumReduceTasks(HADOOP_REDUCERS);

		conf.set("mapred.child.java.opts", HADOOP_JAVAOPTS);
		conf.set("mapred.task.timeout", Long.toString(HADOOP_TIMEOUT));

		if (HADOOP_MOCOMP) {
			// Hadoop 0.20 and before
			conf.setBoolean("mapred.compress.map.output", true);
			// Hadoop 0.21 and later
			conf.setBoolean("mapreduce.map.output.compress", true);

			if (conf.get("io.compression.codecs") != null) {
				if (conf.get("io.compression.codecs").contains(CODECS.SNAPPY)) {
					// Hadoop 0.20 and before
					conf.set("mapred.map.output.compression.codec",
							CODECS.SNAPPY);
					// Hadoop 0.21 and later
					conf.set("mapreduce.map.output.compress.codec",
							CODECS.SNAPPY);
				} else if (conf.get("io.compression.codecs").contains(
						CODECS.LZO)) {
					// Hadoop 0.20 and before
					conf.set("mapred.map.output.compression.codec", CODECS.LZO);
					// Hadoop 0.21 and later
					conf.set("mapreduce.map.output.compress.codec", CODECS.LZO);
				}
			}
		}

		if (HADOOP_OUTCOMP) {
			// Hadoop 0.20 and before
			conf.setBoolean("mapred.output.compress", true);
			// Hadoop 0.21 and later
			conf.setBoolean("mapreduce.output.compress", true);

			if (conf.get("io.compression.codecs") != null) {
				if (conf.get("io.compression.codecs").contains(CODECS.SNAPPY)) {
					// Hadoop 0.20 and before
					conf.set("mapred.output.compression.codec", CODECS.SNAPPY);
					// Hadoop 0.21 and later
					conf.set("mapreduce.output.compression.codec",
							CODECS.SNAPPY);
				} else if (conf.get("io.compression.codecs").contains(
						CODECS.LZO)) {
					// Hadoop 0.20 and before
					conf.set("mapred.output.compression.codec", CODECS.LZO);
					// Hadoop 0.21 and later
					conf.set("mapreduce.output.compression.codec", CODECS.LZO);
				}
			}
		}

		conf.setInt("STACK_MAX", STACK_MAX);
		conf.setInt("STACK_MIN", STACK_MIN);
		conf.setInt("K", K);

		conf.set("ARM_SCHEME", ARM_SCHEME);
		conf.setInt("ARM_L", ARM_L);
		conf.setInt("ARM_H", ARM_H);

		conf.set("PINCHCORRECT", PINCHCORRECT);
		conf.setBoolean("FILTER_P", FILTER_P);
		conf.setBoolean("FILTER_S", FILTER_S);

		conf.set("SHAVE_IGNORE", SHAVE_IGNORE);
		conf.set("MERGE_IGNORE", MERGE_IGNORE);
	}

	public static void printConfiguration() {
		validateConfiguration();

		CloudEC.msg("==================================================================================\n");
		CloudEC.msg("HDFS_IN:   " + hadoopReadPath + "\n");
		CloudEC.msg("HDFS_OUT:  " + hadoopBasePath + "\n");
		CloudEC.msg("LOCAL_OUT: " + localBasePath + "\n");
		CloudEC.msg("\n");

		CloudEC.msg("HADOOP_MAPPERS    = " + HADOOP_MAPPERS + "\n");
		CloudEC.msg("HADOOP_REDUCERS   = " + HADOOP_REDUCERS + "\n");
		CloudEC.msg("HADOOP_JAVA_OPTS  = " + HADOOP_JAVAOPTS + "\n");
		CloudEC.msg("HADOOP_TIMEOUT    = " + HADOOP_TIMEOUT + "\n");
		CloudEC.msg("\n");

		if (STACK_MIN == -1 && STACK_MAX == -1) {
			CloudEC.msg("STACK SIZE  = NO BOUND\n");
		} else {
			CloudEC.msg("STACK SIZE  = [");
			CloudEC.msg((STACK_MIN == -1 ? "NO BOUND" : STACK_MIN) + ", ");
			CloudEC.msg((STACK_MAX == -1 ? "NO BOUND" : STACK_MAX) + "]\n");
		}
		CloudEC.msg("KMER LENGTH = " + K + "\n");
		CloudEC.msg("\n");

		CloudEC.msg("ARM SCHEME  = ");
		if ("".equals(ARM_SCHEME)) {
			CloudEC.msg(ARM_L == -1 ? "DEFAULT" : "BLA");
		} else {
			CloudEC.msg(ARM_SCHEME);
		}
		CloudEC.msg("\n");
		CloudEC.msg("ARM LENGTH  = " + (ARM_L == -1 ? "NO BOUND" : ARM_L) + "\n");
		if (ARM_SCHEME.equals("GNV")) {
			CloudEC.msg("ARM HEIGHT  = " + ARM_H + "\n");
		}
		CloudEC.msg("\n");

		if (STARTSTAGE != null) {
			CloudEC.msg("STARTSTAGE              = " + STARTSTAGE + "\n");
		}
		if (STOPSTAGE != null) {
			CloudEC.msg("STOPSTAGE               = " + STOPSTAGE + "\n");
		}
		CloudEC.msg("STAGE PINCHCORRECT      = " + PINCHCORRECT + "\n");
		CloudEC.msg("STAGE LARGEKMERFILTER   = " + LARGEKMERFILTER + "\n");
		CloudEC.msg("STAGE SPREADCORRECT     = " + SPREADCORRECT + "\n");
		CloudEC.msg("STAGE UNIQUEKMERFILTER  = " + UNIQUEKMERFILTER + "\n");
		CloudEC.msg("\n");

		CloudEC.msg("RUNS PINCHCORRECT  = " + PCRUN + "\n");
		CloudEC.msg("RUNS SPREADCORRECT = " + SCRUN + "\n");
		CloudEC.msg("\n");

		CloudEC.msg("SHAVE IGNORE READ  = " + SHAVE_IGNORE + "\n");
		CloudEC.msg("MERGE IGNORED READ = " + MERGE_IGNORE + "\n");

		CloudEC.msg("==================================================================================\n");
		CloudEC.msg("\n");
	}

	@SuppressWarnings("static-access")
	public static void parseOptions(String[] args) {
		Options options = new Options();

		options.addOption(new Option("help", "print this message"));
		options.addOption(new Option("h", "print this message"));
		options.addOption(new Option("expert", "show expert options"));

		// working directories
		options.addOption(OptionBuilder
				.withArgName("out")
				.hasArg()
				.withDescription(
						"Output directory of corrected reads (required)")
				.create("out"));
		options.addOption(OptionBuilder.withArgName("in").hasArg()
				.withDescription("Input directory of reads (required)")
				.create("in"));
		options.addOption(OptionBuilder
				.withArgName("work")
				.hasArg()
				.withDescription(
						"Local directory of log files (default: "
								+ localBasePath + ")").create("work"));

		// hadoop options
		options.addOption(OptionBuilder
				.withArgName("mtasks")
				.hasArg()
				.withDescription(
						"Number of Map Tasks (default: " + HADOOP_MAPPERS + ")")
				.create("mtasks"));
		options.addOption(OptionBuilder
				.withArgName("rtasks")
				.hasArg()
				.withDescription(
						"Number of Reduce Tasks (default: " + HADOOP_REDUCERS
								+ ")").create("rtasks"));
		options.addOption(OptionBuilder
				.withArgName("javaopts")
				.hasArg()
				.withDescription(
						"Hadoop Java Options (default: " + HADOOP_JAVAOPTS
								+ ")").create("javaopts"));
		options.addOption(OptionBuilder
				.withArgName("timeout")
				.hasArg()
				.withDescription(
						"Hadoop task timeout (default: " + HADOOP_TIMEOUT + ")")
				.create("timeout"));
		options.addOption(OptionBuilder
				.withArgName("mocomp")
				.hasArg()
				.withDescription(
						"Compress MapReduce intermediate data (default: "
								+ HADOOP_MOCOMP + ")").create("mocomp"));
		options.addOption(OptionBuilder
				.withArgName("outcomp")
				.hasArg()
				.withDescription(
						"Compress MapReduce output (default: " + HADOOP_OUTCOMP
								+ ")").create("outcomp"));

		// Stage control
		options.addOption(OptionBuilder.withArgName("start").hasArg()
				.withDescription("Start stage").create("start"));
		options.addOption(OptionBuilder.withArgName("stop").hasArg()
				.withDescription("Stop stage").create("stop"));

		options.addOption(OptionBuilder.withArgName("pinchcorrect").hasArg()
				.withDescription("PinchCorrect stage").create("pinchcorrect"));
		options.addOption(OptionBuilder.withArgName("largekmerfilter").hasArg()
				.withDescription("LargeKmerFilter stage").create("largekmerfilter"));
		options.addOption(OptionBuilder.withArgName("spreadcorrect").hasArg()
				.withDescription("SpreadCorrect stage").create("spreadcorrect"));
		options.addOption(OptionBuilder.withArgName("uniquekmerfilter").hasArg()
				.withDescription("UniqueKmerFilter stage").create("uniquekmerfilter"));

		options.addOption(OptionBuilder
				.withArgName("pcrun")
				.hasArg()
				.withDescription(
						"Number of runs in the PinchCorrect Stage (default: "
								+ PCRUN + ")").create("pcrun"));
		options.addOption(OptionBuilder
				.withArgName("scrun")
				.hasArg()
				.withDescription(
						"Number of runs in the SpreadCorrect Stage (default: "
								+ SCRUN + ")").create("scrun"));

		// global parameters
		options.addOption(OptionBuilder.withArgName("K").hasArg()
				.withDescription("Length of a Kmer").create("K"));

		// scheme parameters
		options.addOption(OptionBuilder.withArgName("arm_scheme").hasArg()
				.withDescription("Alternative process scheme (CLA|ENV|GNV)")
				.create("arm_scheme"));
		options.addOption(OptionBuilder.withArgName("arm_l").hasArg()
				.withDescription("Length of the arms around a Kmer")
				.create("arm_l"));
		options.addOption(OptionBuilder.withArgName("arm_h").hasArg()
				.withDescription("Height of the arms of the stack")
				.create("arm_h"));

		// kmer status
		options.addOption(OptionBuilder
				.withArgName("stackmax")
				.hasArg()
				.withDescription(
						"Maximum items of a readstack (default: " + STACK_MAX
								+ ")").create("stackmax"));
		options.addOption(OptionBuilder
				.withArgName("stackmin")
				.hasArg()
				.withDescription(
						"Minimum items of a readstack (default: " + STACK_MIN
								+ ")").create("stackmin"));

		// input status
		options.addOption(OptionBuilder
				.withArgName("filterreads")
				.hasArg()
				.withDescription(
						"Filter (ignore) reads have N bases from input (default: "
								+ SHAVE_IGNORE + ")").create("filterreads"));

		// output status
		options.addOption(OptionBuilder
				.withArgName("mergereads")
				.hasArg()
				.withDescription(
						"Merge ignored reads from input (default: "
								+ MERGE_IGNORE + ")").create("mergereads"));

		CommandLineParser parser = new GnuParser();

		try {
			CommandLine line = parser.parse(options, args);

			if (line.hasOption("help") || line.hasOption("h")
					|| line.hasOption("expert")) {
				System.out
						.print("Usage: hadoop jar ReadStackCorrector.jar [-in dir] [-out dir] [options]\n"
								+ "\n"
								+ "General Options:\n"
								+ "===============\n"
								+ "  -out <outdir>    : Output directory of corrected reads (required)\n"
								+ "  -in <indir>      : Input directory of reads (required)\n"
								+ "  -work <workdir>  : Local directory of log files ["
								+ localBasePath
								+ "]\n"
								+ "  -K <size>        : Length of a Kmer ["
								+ K
								+ "]\n"
								+ "  -expert          : Show expert options\n");

				if (line.hasOption("expert")) {
					System.out
							.print("  -stackmax <size> : Maximum items of a readstack ["
									+ STACK_MAX
									+ "]\n"
									+ "  -stackmin <size> : Minimum items of a readstack ["
									+ STACK_MIN
									+ "]\n"
									+ "\n"
									+ "Scheme Options:\n"
									+ "===============\n"
									+ "  -arm_scheme <name>  : Alternative process scheme (CLA|ENV|GNV) ["
									+ "DEFAULT"
									+ "]\n"
									+ "  -arm_l <length>     : Length of the arms around a Kmer ["
									+ ARM_L
									+ "]\n"
									+ "  -arm_h <height>     : Height of the arms of the stack ["
									+ ARM_H
									+ "]\n"
									+ "\n"
									+ "Stage Options:\n"
									+ "===============\n"
									+ "  -pinchcorrect <on/off>     : Switch of the PinchCorrect Stage ["
									+ PINCHCORRECT
									+ "]\n"
									+ "  -largekmerfilter <on/off>  : Switch of the LargeKmerFilter Stage ["
									+ LARGEKMERFILTER
									+ "]\n"
									+ "  -spreadcorrect <on/off>    : Switch of the SpreadCorrect Stage ["
									+ SPREADCORRECT
									+ "]\n"
									+ "  -uniquekmerfilter <on/off> : Switch of the UniqueKmerFilter Stage ["
									+ UNIQUEKMERFILTER
									+ "]\n"
									+ "  -pcrun <number>            : Number of runs in the PinchCorrect Stage ["
									+ PCRUN
									+ "]\n"
									+ "  -scrun <number>            : Number of runs in the SpreadCorrect Stage ["
									+ SCRUN
									+ "]\n"
									+ "  -filterreads <on/off>      : Filter (ignore) reads have N bases from input ["
									+ SHAVE_IGNORE
									+ "]\n"
									+ "  -mergereads <on/off>       : Merge ignored reads from input ["
									+ MERGE_IGNORE
									+ "]\n"
									+ "\n"
									+ "Hadoop Options:\n"
									+ "===============\n"
									+ "  -javaopts <opts>    : Hadoop Java Options ["
									+ HADOOP_JAVAOPTS
									+ "]\n"
									+ "  -mtasks <number>    : Number of Map Tasks ["
									+ HADOOP_MAPPERS
									+ "]\n"
									+ "  -rtasks <number>    : Number of Reduce Tasks ["
									+ HADOOP_REDUCERS
									+ "]\n"
									+ "  -mocomp <on/off>    : Compress MapReduce intermediate data ["
									+ (HADOOP_MOCOMP == true ? "on" : "off")
									+ "]\n"
									+ "  -outcomp <on/off>   : Compress MapReduce output ["
									+ (HADOOP_OUTCOMP == true ? "on" : "off")
									+ "]\n"
									+ "  -timeout <usec>     : Hadoop task timeout ["
									+ HADOOP_TIMEOUT + "]\n" + "\n");
				}

				System.exit(0);
			}
			if (line.hasOption("out")) {
				hadoopBasePath = line.getOptionValue("out");
			}
			if (line.hasOption("in")) {
				hadoopReadPath = line.getOptionValue("in");
			}
			if (line.hasOption("work")) {
				localBasePath = line.getOptionValue("work");
			}
			if (line.hasOption("mtasks")) {
				HADOOP_MAPPERS = Integer
						.parseInt(line.getOptionValue("mtasks"));
			}
			if (line.hasOption("rtasks")) {
				HADOOP_REDUCERS = Integer.parseInt(line
						.getOptionValue("rtasks"));
			}
			if (line.hasOption("javaopts")) {
				HADOOP_JAVAOPTS = line.getOptionValue("javaopts");
			}
			if (line.hasOption("mocomp")) {
				if (line.getOptionValue("mocomp").equals("off")) {
					HADOOP_MOCOMP = false;
				}
			}
			if (line.hasOption("outcomp")) {
				if (line.getOptionValue("outcomp").equals("off")) {
					HADOOP_OUTCOMP = false;
				}
			}
			if (line.hasOption("timeout")) {
				HADOOP_TIMEOUT = Long.parseLong(line.getOptionValue("timeout"));
			}
			if (line.hasOption("K")) {
				K = Integer.parseInt(line.getOptionValue("K"));
			}
			if (line.hasOption("stackmax")) {
				STACK_MAX = Integer.parseInt(line.getOptionValue("stackmax"));
			}
			if (line.hasOption("stackmin")) {
				STACK_MIN = Integer.parseInt(line.getOptionValue("stackmin"));
			}
			if (line.hasOption("arm_scheme")) {
				ARM_SCHEME = line.getOptionValue("arm_scheme").trim().toUpperCase();
			}
			if (line.hasOption("arm_l")) {
				ARM_L = Integer.parseInt(line.getOptionValue("arm_l"));
			}
			if (line.hasOption("arm_h")) {
				ARM_H = Integer.parseInt(line.getOptionValue("arm_h"));
			}
			if (line.hasOption("start")) {
				STARTSTAGE = line.getOptionValue("start");
			}
			if (line.hasOption("stop")) {
				STOPSTAGE = line.getOptionValue("stop");
			}
			if (line.hasOption("largekmerfilter")) {
				LARGEKMERFILTER = line.getOptionValue("largekmerfilter");
			}
			if (line.hasOption("pinchcorrect")) {
				PINCHCORRECT = line.getOptionValue("pinchcorrect");
				if (PINCHCORRECT.equals("off")) {
					FILTER_P = false;
				}
			}
			if (line.hasOption("spreadcorrect")) {
				SPREADCORRECT = line.getOptionValue("spreadcorrect");
				if (SPREADCORRECT.equals("off")) {
					FILTER_S = false;
				}
			}
			if (line.hasOption("uniquekmerfilter")) {
				UNIQUEKMERFILTER = line.getOptionValue("uniquekmerfilter");
			}
			if (line.hasOption("pcrun")) {
				PCRUN = Integer.parseInt(line.getOptionValue("pcrun"));
			}
			if (line.hasOption("scrun")) {
				SCRUN = Integer.parseInt(line.getOptionValue("scrun"));
			}
			if (line.hasOption("filterreads")) {
				SHAVE_IGNORE = line.getOptionValue("filterreads");
			}
			if (SHAVE_IGNORE.equals("on")) {
				if (line.hasOption("mergereads")) {
					MERGE_IGNORE = line.getOptionValue("mergereads");
				}
			}
		} catch (ParseException exp) {
			System.err.println("Parsing failed.  Reason: " + exp.getMessage());
			System.exit(1);
		}

	}

}
