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

public class PinchCorrectDecision extends Configured implements Tool {
	private static final Logger sLogger = Logger.getLogger(PinchCorrectDecision.class);

	public static class PinchCorrectDecisionMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable lineid, Text nodetxt,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			String vals[] = nodetxt.toString().split("\t", 2);

			output.collect(new Text(vals[0]), new Text(vals[1]));
		}
	}

	// FIXME: code need review
	public static class PinchCorrectDecisionReducer extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		private static int K = 0;

		@Override
		public void configure(JobConf job) {
			K = job.getInt("K", 0);
		}

		public class Correct {
			public char chr;
			public int pos;

			public Correct(int pos1, char chr1) throws IOException {
				pos = pos1;
				chr = chr1;
			}
		}

		@Override
		public void reduce(Text nodeid, Iterator<Text> iter,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			Utils node = new Utils(nodeid.toString());

			ArrayList<Correct> msgs = new ArrayList<Correct>();

			int mainnode = 0;

			while (iter.hasNext()) {
				String[] vals = iter.next().toString().split("\t");

				if (vals[0].equals(Utils.MSGNODE)) {
					node.parseNodeMsg(vals, 0);
					mainnode++;
				} else if (vals[0].equals(Utils.MSGCORRECT)) {
					String[] vals2 = Utils.corrDecode(vals[1]).split("X");

					for (int i = 0; i < vals2.length; i++) {
						if (vals2[i].length() != 0) {
							for (Character c : vals2[i].toCharArray()) {
								msgs.add(new Correct(i, c));
							}
						}
					}
				}
			}

			// zero or multiple main node exists, it's an error
			if (mainnode != 1) {
				msgs.clear();

				return;
			}

			if (msgs.size() > 0) {
				// array: [0]=A, [1]=T, [2]=C, [3]=G, [4]=Sum
				int[][] array = new int[node.getLen()][5];

				boolean[] skip = new boolean[node.getLen()];

				for (int i = 0; i < node.getLen(); i++) {
					skip[i] = false;

					for (int j = 0; j < 5; j++) {
						array[i][j] = 0;
					}
				}

				for (int i = 0; i < msgs.size(); i++) {
					int pos = msgs.get(i).pos;
					int base = Utils.char2idx(msgs.get(i).chr);

					array[pos][base]++;

					if (base != Utils.char2idx('N')) {
						array[pos][4]++;
					}
				}

				// check if corrections are far away enough
				for (int i = 0; i < array.length; i++) {
					if (array[i][4] != 0) {
						for (int j = i + 1; j < array.length; j++) {
							if (array[j][4] != 0) {
								if (j - i <= K / 2) {
									skip[i] = true;
									skip[j] = true;
								}

								i = j - 1;

								break;
							}
						}
					}
				}

				// fix content
				StringBuilder fix_str = new StringBuilder(node.getSEQ());
				StringBuilder fix_qv = new StringBuilder(node.getQV());

				boolean corrected = false;

				for (int i = 0; i < array.length; i++) {
					if (array[i][4] > 0) {
						char fix_char = 'X';

						// The recommendation must be there
						for (int base = 0; base < 4; base++) {
							if (array[i][base] == array[i][4]) {
								fix_char = Utils.idx2char(base);
								break;
							}
						}

						if (fix_char != 'X') {
							if (!skip[i]) {
								fix_str.setCharAt(i, fix_char);
								fix_qv.setCharAt(i, (char) (Utils.QV_FIX));

								corrected = true;

								reporter.incrCounter("Brush", "fix_char", 1);
							} else {
								reporter.incrCounter("Brush", "skip_char", 1);
							}
						}
					}
				}

				if (corrected) {
					reporter.incrCounter("Brush", "fix_read", 1);
				}

				node.setSEQ(fix_str.toString());
				node.setQV(fix_qv.toString());
			}

			msgs.clear();

			// TODO: remove filter data in the last run

			output.collect(nodeid, new Text(node.toNodeMsg()));
		}
	}

	public RunningJob run(String inputPath, String outputPath) throws Exception {
		sLogger.info("Tool name: " + PinchCorrectDecision.class.getSimpleName() + "  [1/5]");
		sLogger.info(" - input: " + inputPath);
		sLogger.info(" - output: " + outputPath);

		JobConf conf = new JobConf(PinchCorrectDecision.class);
		conf.setJobName(PinchCorrectDecision.class.getSimpleName() + " " + inputPath);

		Config.initializeConfiguration(conf);

		FileInputFormat.addInputPaths(conf, inputPath);
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(PinchCorrectDecisionMapper.class);
		conf.setReducerClass(PinchCorrectDecisionReducer.class);

		// delete the output directory if it exists already
		FileSystem.get(conf).delete(new Path(outputPath), true);

		return JobClient.runJob(conf);
	}

	@Override
	public int run(String[] args) throws Exception {
		String inputPath = "";
		String outputPath = "";

		run(inputPath, outputPath);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new PinchCorrectDecision(), args);
		System.exit(res);
	}
}
