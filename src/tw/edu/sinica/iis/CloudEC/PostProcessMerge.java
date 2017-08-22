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

public class PostProcessMerge extends Configured implements Tool {
	private static final Logger sLogger = Logger.getLogger(PostProcessMerge.class);

	public static class PostProcessMergeMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable lineid, Text nodetxt,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			String vals[] = nodetxt.toString().split("\t", 2);

			output.collect(new Text(vals[0]), new Text(vals[1]));
		}
	}

	public static class PostProcessMergeReducer extends MapReduceBase
			implements Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text nodeid, Iterator<Text> iter,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			ArrayList<Utils> data = new ArrayList<Utils>();

			int main = -1;

			while (iter.hasNext()) {
				String[] vals = iter.next().toString().split("\t");

				Utils node = new Utils(nodeid.toString());

				if (vals[0].equals(Utils.MSGNODE)) {
					node.parseNodeMsg(vals, 0);
					main = data.size();
				} else if (vals.length == 2) {
					node.setSEQ(vals[0].toUpperCase());
					node.setQV(Utils.qvInputConvert(vals[1]));
					node.setCoverage(1);
				} else {
					continue;
				}

				data.add(node);
			}

			if (data.size() == 1) {
				output.collect(new Text(data.get(0).getNodeId()),
						new Text(data.get(0).toNodeMsg()));

				reporter.incrCounter("Brush", "reads_ign", 1);
			} else if (data.size() == 2 && main != -1) {
				output.collect(new Text(data.get(main).getNodeId()),
						new Text(data.get(main).toNodeMsg()));

				reporter.incrCounter("Brush", "reads_ec", 1);
			} else {
				reporter.incrCounter("Brush", "reads_fail", 1);
			}

			data.clear();

		}
	}

	public RunningJob run(String inputPath, String outputPath) throws Exception {
		sLogger.info("Tool name: " + PostProcessMerge.class.getSimpleName() + " [5/5]");
		sLogger.info(" - input: " + inputPath);
		sLogger.info(" - output: " + outputPath);

		JobConf conf = new JobConf(PostProcessMerge.class);
		conf.setJobName(PostProcessMerge.class.getSimpleName() + " " + inputPath);

		Config.initializeConfiguration(conf);

		FileInputFormat.addInputPaths(conf, inputPath);
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(PostProcessMergeMapper.class);
		conf.setReducerClass(PostProcessMergeReducer.class);

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
		int res = ToolRunner.run(new Configuration(), new PostProcessMerge(), args);
		System.exit(res);
	}
}
