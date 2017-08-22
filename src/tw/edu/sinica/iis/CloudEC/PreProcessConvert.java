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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class PreProcessConvert extends Configured implements Tool {
	private static final Logger sLogger = Logger
			.getLogger(PreProcessConvert.class);

	public static class PreProcessConvertMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		private static int K = 0;
		private static boolean PINCHCORRECT = true;
		private static boolean FILTERREADS = true;

		@Override
		public void configure(JobConf job) {
			K = job.getInt("K", 0);
			PINCHCORRECT = (job.get("PINCHCORRECT", "on").equals("on") ? true : false);
			FILTERREADS = (job.get("SHAVE_IGNORE", "on").equals("on") ? true : false);
		}

		@Override
		public void map(LongWritable lineid, Text nodetxt,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			String[] fields = nodetxt.toString().split("\t");

			if (fields.length != 3) {
				return;
			}

			String tag = fields[0];
			String seq = fields[1].toUpperCase();
			String qscore = fields[2].toString();

			// filter out inconsistent reads
			if (seq.length() != qscore.length()) {
				reporter.incrCounter("Brush", "reads_skipped", 1);
				return;
			}

			// filter out read of length < K
			if (seq.length() < K + (PINCHCORRECT ? 1 : 0)) {
				reporter.incrCounter("Brush", "reads_skipped", 1);
				return;
			}

			// filter out sequences have N bases, or has highly N bases
			if (FILTERREADS) {
				if (seq.matches(".*[^ACGT].*")) {
					reporter.incrCounter("Brush", "reads_skipped", 1);
					return;
				}
			} else {
				if (Utils.seqIsPoly(seq, 'N')) {
					reporter.incrCounter("Brush", "reads_poly", 1);
					return;
				}
			}

			// filter out the poly-A reads
			if (Utils.seqIsPoly(seq, 'A')) {
				reporter.incrCounter("Brush", "reads_poly", 1);
				return;
			}

			// Convert read/qv to internal format, and emit
			Utils node = new Utils(tag);

			node.setSEQ(seq);
			node.setQV(Utils.qvInputConvert(qscore));
			node.setCoverage(1);

			reporter.incrCounter("Brush", "reads_good", 1);
			reporter.incrCounter("Brush", "reads_goodbp", node.getLen());
			reporter.incrCounter("Brush", "reads_gccnts", node.getGCCnt());

			output.collect(new Text(node.getNodeId()), new Text(node.toNodeMsg()));
		}
	}

	public RunningJob run(String inputPath, String outputPath) throws Exception {
		sLogger.info("Tool name: " + PreProcessConvert.class.getSimpleName() + " [0/5]");
		sLogger.info(" - input: " + inputPath);
		sLogger.info(" - output: " + outputPath);

		JobConf conf = new JobConf(PreProcessConvert.class);
		conf.setJobName(PreProcessConvert.class.getSimpleName() + " " + inputPath + " " + Config.K);

		Config.initializeConfiguration(conf);

		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(PreProcessConvertMapper.class);
		conf.setNumReduceTasks(0);

		// delete the output directory if it exists already
		FileSystem.get(conf).delete(new Path(outputPath), true);

		return JobClient.runJob(conf);
	}

	@Override
	public int run(String[] args) throws Exception {
		String inputPath = "";
		String outputPath = "";

		long starttime = System.currentTimeMillis();

		run(inputPath, outputPath);

		long endtime = System.currentTimeMillis();

		float diff = (float) ((endtime - starttime) / 1000.0);

		System.out.println("Runtime: " + diff + " s");

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new PreProcessConvert(),
				args);
		System.exit(res);
	}
}
