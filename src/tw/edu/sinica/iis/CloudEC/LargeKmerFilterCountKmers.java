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
import java.util.ArrayList;
import java.util.Iterator;

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
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class LargeKmerFilterCountKmers extends Configured implements Tool {
	private static final Logger sLogger = Logger
			.getLogger(LargeKmerFilterCountKmers.class);

	public static class LargeKmerFilterCountKmersMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		public static int K = 0;

		public static boolean FILTER_P = true;
		public static boolean FILTER_S = true;

		@Override
		public void configure(JobConf job) {
			K = job.getInt("K", 0);

			FILTER_P = job.getBoolean("FILTER_P", true);
			FILTER_S = job.getBoolean("FILTER_S", true);
		}

		@Override
		public void map(LongWritable lineid, Text nodetxt,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			Utils node = new Utils();
			node.fromNodeMsg(nodetxt.toString());

			String nodeStr = node.getSEQ();

			// generate (k+1)-mers for PinchCorrect
			if (FILTER_P) {
				int end = nodeStr.length() - (K + 1) + 1;
				for (int i = 0; i < end; i++) {
					String window_f = nodeStr.substring(i, i + (K / 2))
							+ nodeStr.substring(i + (K / 2) + 1, i + K + 1);
					String window_r = Utils.rcSEQ(window_f);

					if (window_f.contains("N")) {
						continue;
					}

					int compare = window_f.compareTo(window_r);
					if (compare < 0) {
						output.collect(
								new Text(Utils.MSGIGNP
										+ Utils.seqEncode(window_f)),
								new Text(node.getNodeId() + "\t" + i));
					} else {
						output.collect(
								new Text(Utils.MSGIGNP
										+ Utils.seqEncode(window_r)),
								new Text(node.getNodeId() + "\t" + i));
					}
				}
			}

			// generate k-mers for FindError
			if (FILTER_S) {
				int end = nodeStr.length() - K + 1;
				for (int i = 0; i < end; i++) {
					String window_f = nodeStr.substring(i, i + K);
					String window_r = Utils.rcSEQ(window_f);

					if (window_f.contains("N")) {
						continue;
					}

					int compare = window_f.compareTo(window_r);
					if (compare < 0) {
						output.collect(
								new Text(Utils.MSGIGNF
										+ Utils.seqEncode(window_f)),
								new Text(node.getNodeId() + "\t" + i));
					} else {
						output.collect(
								new Text(Utils.MSGIGNF
										+ Utils.seqEncode(window_r)),
								new Text(node.getNodeId() + "\t" + i));
					}
				}
			}
		}
	}

	public static class LargeKmerFilterCountKmersReducer extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		public static int StackMax = 0;
		public static int StackMin = 0;

		@Override
		public void configure(JobConf job) {
			StackMax = job.getInt("STACK_MAX", 0);
			StackMin = job.getInt("STACK_MIN", 0);
		}

		@Override
		public void reduce(Text prefix, Iterator<Text> iter,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			ArrayList<String> kmerlist = new ArrayList<String>();

			while (iter.hasNext()) {
				kmerlist.add(iter.next().toString());
			}

			if ((StackMax != -1 && kmerlist.size() > StackMax)
					|| (StackMin != -1 && kmerlist.size() < StackMin)) {
				// Ignore message type: PinchCorrect (P) or FindError (F)
				String IGNType = prefix.toString().substring(0, 1);

				for (String s : kmerlist) {
					// data: [0]=id, [1]=pos
					String[] data = s.split("\t");

					output.collect(new Text(data[0]), new Text(IGNType + "\t"
							+ data[1]));
				}

				if (StackMax != -1 && kmerlist.size() > StackMax) {
					reporter.incrCounter("Brush", "hkmer", 1);
				} else if (StackMin != -1 && kmerlist.size() < StackMin) {
					reporter.incrCounter("Brush", "lkmer", 1);
				}
			}

			kmerlist.clear();
		}
	}

	public RunningJob run(String inputPath, String outputPath) throws Exception {
		sLogger.info("Tool name: " + LargeKmerFilterCountKmers.class.getSimpleName() + "  [2/5]");
		sLogger.info(" - input: " + inputPath);
		sLogger.info(" - output: " + outputPath);

		JobConf conf = new JobConf(LargeKmerFilterCountKmers.class);
		conf.setJobName(LargeKmerFilterCountKmers.class.getSimpleName() + " " + inputPath + " " + Config.K);

		Config.initializeConfiguration(conf);

		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(LargeKmerFilterCountKmersMapper.class);
		conf.setReducerClass(LargeKmerFilterCountKmersReducer.class);

		// delete the output directory if it exists already
		FileSystem.get(conf).delete(new Path(outputPath), true);

		return JobClient.runJob(conf);
	}

	@Override
	public int run(String[] args) throws Exception {
		String inputPath = "";
		String outputPath = "";
		Config.K = 24;

		long starttime = System.currentTimeMillis();

		run(inputPath, outputPath);

		long endtime = System.currentTimeMillis();

		float diff = (float) ((endtime - starttime) / 1000.0);

		System.out.println("Runtime: " + diff + " s");

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new LargeKmerFilterCountKmers(),
				args);
		System.exit(res);
	}
}
