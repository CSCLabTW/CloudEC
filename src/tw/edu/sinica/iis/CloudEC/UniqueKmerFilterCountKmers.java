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

public class UniqueKmerFilterCountKmers extends Configured implements Tool {
	private static final Logger sLogger = Logger
			.getLogger(UniqueKmerFilterCountKmers.class);

	public static class UniqueKmerFilterCountKmersMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		public static int K = 0;

		@Override
		public void configure(JobConf job) {
			K = job.getInt("K", 0);
		}

		@Override
		public void map(LongWritable lineid, Text nodetxt,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			Utils node = new Utils();
			node.fromNodeMsg(nodetxt.toString());

			int end = node.getLen() - K + 1;
			for (int i = 0; i < end; i++) {
				String window_tmp = node.getSEQ().substring(i, i + K);
				String window_tmp_r = Utils.rcSEQ(window_tmp);

				if (window_tmp.contains("N")) {
					continue;
				}

				int compare = window_tmp.compareTo(window_tmp_r);
				if (compare < 0) {
					output.collect(new Text(Utils.seqEncode(window_tmp)),
							new Text(node.getNodeId()));
				} else {
					output.collect(new Text(Utils.seqEncode(window_tmp_r)),
							new Text(node.getNodeId()));
				}
			}
		}
	}

	public static class UniqueKmerFilterCountKmersReducer extends MapReduceBase
			implements Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text prefix, Iterator<Text> iter,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			// A list contains the read id contain the kmer (prefix)
			ArrayList<String> list = new ArrayList<String>();

			while (iter.hasNext()) {
				list.add(iter.next().toString());

				// ignore when the kmer reach the threshold
				if (list.size() > Utils.UNIQUE_READ) {
					break;
				}
			}

			// sent out id unique reads
			if (list.size() <= Utils.UNIQUE_READ) {
				for (String s : list) {
					output.collect(new Text(s), new Text(Utils.MSGUPDATE + "\t" + 1));
				}
			}

			list.clear();
		}
	}

	public RunningJob run(String inputPath, String outputPath) throws Exception {
		sLogger.info("Tool name: " + UniqueKmerFilterCountKmers.class.getSimpleName() + " [4/5]");
		sLogger.info(" - input: " + inputPath);
		sLogger.info(" - output: " + outputPath);

		JobConf conf = new JobConf(UniqueKmerFilterCountKmers.class);
		conf.setJobName(UniqueKmerFilterCountKmers.class.getSimpleName() + " " + inputPath + " " + Config.K);

		Config.initializeConfiguration(conf);

		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(UniqueKmerFilterCountKmersMapper.class);
		conf.setReducerClass(UniqueKmerFilterCountKmersReducer.class);

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
		int res = ToolRunner.run(new Configuration(), new UniqueKmerFilterCountKmers(),
				args);
		System.exit(res);
	}
}
