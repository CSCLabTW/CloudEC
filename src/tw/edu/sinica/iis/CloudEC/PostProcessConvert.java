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

public class PostProcessConvert extends Configured implements Tool {
	private static final Logger sLogger = Logger.getLogger(PostProcessConvert.class);

	private static class PostProcessConvertMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable lineid, Text nodetxt,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			Utils node = new Utils();
			node.fromNodeMsg(nodetxt.toString());

			// do not output reads containing unique kmer
			if (node.isUnique()) {
				reporter.incrCounter("Brush", "unique_reads", 1);
				return;
			}

			reporter.incrCounter("Brush", "output_reads", 1);

			output.collect(new Text("@" + node.getNodeId()), null);
			output.collect(new Text(node.getSEQ()), null);
			output.collect(new Text("+" + node.getNodeId()), null);
			output.collect(new Text(Utils.qvOutputConvert(node.getQV())), null);
		}
	}

	public RunningJob run(String inputPath, String outputPath) throws Exception {
		sLogger.info("Tool name: " + PostProcessConvert.class.getSimpleName() + " [5/5]");
		sLogger.info(" - input: " + inputPath);
		sLogger.info(" - output: " + outputPath);

		JobConf conf = new JobConf(PostProcessConvert.class);
		conf.setJobName(PostProcessConvert.class.getSimpleName() + " " + inputPath);

		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(PostProcessConvertMapper.class);

		Config.initializeConfiguration(conf);
		conf.setNumReduceTasks(0);

		// Do not compress output in the last stage
		// Hadoop 0.20 and before
		conf.setBoolean("mapred.output.compress", false);
		// Hadoop 0.21 and later
		conf.setBoolean("mapreduce.output.compress", false);

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
		int res = ToolRunner.run(new Configuration(), new PostProcessConvert(), args);
		System.exit(res);
	}
}
