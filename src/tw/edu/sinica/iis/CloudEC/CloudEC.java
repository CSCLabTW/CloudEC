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

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.TTCCLayout;

public class CloudEC extends Configured implements Tool {
	private static DecimalFormat df = new DecimalFormat("0.00");
	private static FileOutputStream logfile;
	private static PrintStream logstream;

	JobConf baseconf = new JobConf(CloudEC.class);

	static String preprocess = "00-preprocess";
	static String pinchcorrect = "01-pinchcorrect";
	static String largekmerfilter = "02-largekmerfilter";
	static String spreadcorrect = "03-spreadcorrect";
	static String uniquekmerfilter = "04-uniquekmerfilter";
	static String postprocess = "05-postprocess";

	// Message Management
	long GLOBALNUMSTEPS = 0;
	long JOBSTARTTIME = 0;

	public void start(String desc) {
		msg(desc + ":\t");
		JOBSTARTTIME = System.currentTimeMillis();
		GLOBALNUMSTEPS++;
	}

	public void end(RunningJob job) throws IOException {
		long endtime = System.currentTimeMillis();
		long diff = (endtime - JOBSTARTTIME) / 1000;

		msg(job.getID().toString() + " " + diff + " s");

		if (!job.isSuccessful()) {
			System.out.println("Job failed.");
			System.exit(1);
		}
	}

	public static void msg(String msg) {
		logstream.print(msg);
		System.out.print(msg);
	}

	public long counter(RunningJob job, String tag) throws IOException {
		return job.getCounters().findCounter("Brush", tag).getValue();
	}

	// Stage Management
	boolean RUNSTAGE = false;
	private String CURRENTSTAGE;

	public boolean runStage(String stage) {
		CURRENTSTAGE = stage;

		if (Config.STARTSTAGE == null || Config.STARTSTAGE.equals(stage)) {
			RUNSTAGE = true;
		}

		return RUNSTAGE;
	}

	public String checkStageExit(final String currStage) {
		if (Config.STOPSTAGE != null && Config.STOPSTAGE.equals(CURRENTSTAGE)) {
			RUNSTAGE = false;
			msg("Stopping after " + Config.STOPSTAGE + "\n");
			System.exit(0);
		}

		return currStage;
	}

	// File Management
	public void cleanup(String path) throws IOException {
		FileSystem.get(baseconf).delete(new Path(path), true);
	}

	public void save_result(String base, String opath, String npath)
			throws IOException {
		msg("  Save result to " + npath + "\n");

		FileSystem.get(baseconf).delete(new Path(base + npath), true);
		FileSystem.get(baseconf).rename(new Path(base + opath),
				new Path(base + npath));
	}

	// PreProcess
	public void preprocess(String inputPath, String basePath, String loadreads)
			throws Exception {
		RunningJob job;

		msg("\nPreProcess:");

		start("\n  Convert");
		PreProcessConvert ppr = new PreProcessConvert();
		job = ppr.run(inputPath, basePath + loadreads);
		end(job);

		long reads_goodbp = counter(job, "reads_goodbp");
		long reads_gccnts = counter(job, "reads_gccnts");
		long reads_good = counter(job, "reads_good");
		long reads_poly = counter(job, "reads_poly");
		long reads_skip = counter(job, "reads_skipped");
		long reads_all = reads_good + reads_poly + reads_skip;

		if (reads_good == 0) {
			throw new IOException("No good reads");
		}

		String frac_reads = df.format(100.0 * reads_good / reads_all);
		String frac_gccnt = df.format(100.0 * reads_gccnts / reads_goodbp);
		msg(" [" + reads_good + " (" + frac_reads + "%) good_reads, "
				+ reads_goodbp + " bp]");
		msg(" [" + reads_poly + " poly_reads, " + reads_skip + " skip_reads, "
				+ reads_all + " total_reads]");
		msg(" [" + frac_gccnt + "% gc content]");

		msg("\n");
	}

	// PinchCorrect
	public void pinchcorrect(String basePath, String input, String output)
			throws Exception {
		RunningJob job;

		msg("\nPinchCorrect:");

		String current = input;
		long fix_char = 0;
		long skip_char = 0;
		long fix_read = 0;
		long round = 0;

		do {
			round++;

			start("\n  Recommend");
			PinchCorrectRecommend pc = new PinchCorrectRecommend();
			job = pc.run(basePath + current, basePath + output + "." + round
					+".msg");
			end(job);

			fix_char = counter(job, "fix_char");
			msg(" [" + fix_char + " fix_chars]");

			start("\n  Decision");
			PinchCorrectDecision pcorr = new PinchCorrectDecision();
			job = pcorr.run(basePath + current + "," + basePath + output + "."
					+ round +".msg", basePath + output + "." + round);
			end(job);

			fix_char = counter(job, "fix_char");
			skip_char = counter(job, "skip_char");
			fix_read = counter(job, "fix_read");
			msg(" [" + fix_char + " fix_chars, " + skip_char + " skip_chars]");
			msg(" [" + fix_read + " fix_reads]");

			current = output + "." + round;
		} while (fix_char > 0 && round < Config.PCRUN);

		msg("\n");

		save_result(basePath, current, output);
	}

	// LargeKmerFilter
	public void largekmerfilter(String basePath, String input, String output)
			throws Exception {
		RunningJob job;

		msg("\nLargeKmerFilter:");

		start("\n  CountKmers");
		LargeKmerFilterCountKmers cik = new LargeKmerFilterCountKmers();
		job = cik.run(basePath + input, basePath + output + ".ign");
		end(job);

		msg(" [" + counter(job, "hkmer") + " HKmer, " + counter(job, "lkmer")
				+ " LKmer]");

		start("\n  TagReads");
		LargeKmerFilterTagReads ail = new LargeKmerFilterTagReads();
		job = ail.run(basePath + input + "," + basePath + output + ".ign",
				basePath + output);
		end(job);

		msg("\n");
	}

	// SpreadCorrect
	public void spreadcorrect(String basePath, String input, String output)
			throws Exception {

		RunningJob job;

		msg("\nSpreadCorrect:");

		String current = input;
		long fix_char = 0;
		long conflict = 0;
		long confirm_char = 0;
		long round = 0;

		do {
			round++;

			start("\n  Recommend");
			SpreadCorrectRecommend fe = new SpreadCorrectRecommend();
			job = fe.run(basePath + current, basePath + output + "." + round
					+ ".fe");
			end(job);

			fix_char = counter(job, "fix_char");
			confirm_char = counter(job, "confirm_char");
			msg(" [" + confirm_char + " confirms, " + fix_char + " fix_chars]");

			if (round > 1 && fix_char == 0) {
				break;
			}

			start("\n  Decision");
			SpreadCorrectDecision corr = new SpreadCorrectDecision();
			job = corr.run(basePath + current + "," + basePath + output + "."
					+ round + ".fe", basePath + output + "." + round);
			end(job);

			fix_char = counter(job, "fix_char");
			conflict = counter(job, "conflict");
			confirm_char = counter(job, "confirms");
			msg(" [" + confirm_char + " confirms, " + fix_char + " fix_chars, "
					+ conflict + " conflicts]");

			current = output + "." + round;
		} while (fix_char > 0 && round < Config.SCRUN);

		msg("\n");

		save_result(basePath, current, output);
	}

	// UniqueKmerFilter
	public void uniquekmerfilter(String basePath, String input, String output)
			throws Exception {
		RunningJob job;

		msg("\nUniqueKmerFilter:");

		start("\n  CountKmers");
		UniqueKmerFilterCountKmers fuk = new UniqueKmerFilterCountKmers();
		job = fuk.run(basePath + input, basePath + output + ".fuk");
		end(job);

		start("\n  TagReads");
		UniqueKmerFilterTagReads tur = new UniqueKmerFilterTagReads();
		job = tur.run(basePath + input + "," + basePath + output + ".fuk",
				basePath + output);
		end(job);

		long unique_reads = counter(job, "unique_reads");
		msg(" [" + unique_reads + " unique_reads]");

		msg("\n");
	}

	// PostProcess
	public void postprocess(String inputPath, String basePath, String input,
			String output, String OutputPath) throws Exception {
		RunningJob job;

		msg("\nPostProcess:");

		String current = input;

		if (Config.MERGE_IGNORE.equals("on")) {
			current = output + ".mo";

			start("\n  Merge");
			PostProcessMerge mo = new PostProcessMerge();
			job = mo.run(inputPath + "," + basePath + input,
					basePath + current);
			end(job);

			long reads_ign = counter(job, "reads_ign");
			long reads_ec = counter(job, "reads_ec");
			long reads_fail = counter(job, "reads_fail");

			msg(" [" + reads_ign + " reads_ign, " + reads_ec + " reads_ec, "
					+ reads_fail + " reads_fail]");
		}

		start("\n  Convert");
		PostProcessConvert g2f = new PostProcessConvert();
		job = g2f.run(basePath + current, OutputPath);
		end(job);

		long unique_reads = counter(job, "unique_reads");
		long output_reads = counter(job, "output_reads");
		msg(" [" + output_reads + " output_reads]");
		msg(" [" + unique_reads + " unique_reads]");

		msg("\n");
	}

	@Override
	public int run(String[] args) throws Exception {
		Config.parseOptions(args);
		Config.validateConfiguration();

		// Setup to use a file appender
		BasicConfigurator.resetConfiguration();

		TTCCLayout lay = new TTCCLayout();
		lay.setDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

		FileAppender fa = new FileAppender(lay, Config.localBasePath
				+ "cloudec.details.log", true);
		fa.setName("File Appender");
		fa.setThreshold(Level.INFO);
		BasicConfigurator.configure(fa);

		logfile = new FileOutputStream(Config.localBasePath + "cloudec.log",
				true);
		logstream = new PrintStream(logfile);

		Config.printConfiguration();

		// Time stamp
		DateFormat dfm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		long ECstarttime = 0;
		long ECendtime = 0;

		msg("== Starting time " + dfm.format(new Date()) + "\n");

		ECstarttime = System.currentTimeMillis();

		String currStage = Config.hadoopTmpPath;

		if (runStage("preprocess")) {
			preprocess(Config.hadoopReadPath, currStage, preprocess);
		}
		currStage = checkStageExit(preprocess);

		if (Config.PINCHCORRECT.equals("on")) {
			if (runStage("pinchcorrect")) {
				pinchcorrect(Config.hadoopTmpPath, currStage, pinchcorrect);
			}
			currStage = checkStageExit(pinchcorrect);
		}

		if (Config.LARGEKMERFILTER.equals("on")) {
			if (runStage("largekmerfilter")) {
				largekmerfilter(Config.hadoopTmpPath, currStage, largekmerfilter);
			}
			currStage = checkStageExit(largekmerfilter);
		}

		if (Config.SPREADCORRECT.equals("on")) {
			if (runStage("spreadcorrect")) {
				spreadcorrect(Config.hadoopTmpPath, currStage, spreadcorrect);
			}
			currStage = checkStageExit(spreadcorrect);
		}

		if (Config.UNIQUEKMERFILTER.equals("on")) {
			if (runStage("uniquekmerfilter")) {
				uniquekmerfilter(Config.hadoopTmpPath, currStage, uniquekmerfilter);
			}
			currStage = checkStageExit(uniquekmerfilter);
		}

		if (runStage("postprocess")) {
			postprocess(Config.hadoopReadPath, Config.hadoopTmpPath, currStage,
					postprocess, Config.hadoopBasePath);
		}
		currStage = checkStageExit(null);

		ECendtime = System.currentTimeMillis();

		long ecduration = (ECendtime - ECstarttime) / 1000;

		msg("\n");
		msg("== Ending time " + dfm.format(new Date()) + "\n");
		msg("== Duration: " + ecduration + " s, " + GLOBALNUMSTEPS
				+ " total steps, CloudEC\n");

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new CloudEC(), args);
		System.exit(res);
	}
}
