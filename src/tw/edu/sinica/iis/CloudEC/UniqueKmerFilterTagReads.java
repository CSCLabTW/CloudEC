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

public class UniqueKmerFilterTagReads extends Configured implements Tool {
	private static final Logger sLogger = Logger
			.getLogger(UniqueKmerFilterTagReads.class);

	public static class UniqueKmerFilterTagReadsMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable lineid, Text nodetxt,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			String vals[] = nodetxt.toString().split("\t", 2);

			output.collect(new Text(vals[0]), new Text(vals[1]));
		}
	}

	public static class UniqueKmerFilterTagReadsReducer extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text prefix, Iterator<Text> iter,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			Utils node = new Utils();
			boolean trust = true;

			// TODO: need better processing strategy for mixed data
			while (iter.hasNext()) {
				String[] data = iter.next().toString().split("\t", 2);

				if (data[0].equals(Utils.MSGNODE)) {
					node.fromNodeMsg(prefix.toString() + "\t" + data[0] + "\t"
							+ data[1]);
				} else if (data[0].equals(Utils.MSGUPDATE)) {
					trust = false;
				}
			}

			if (trust) {
				node.setOrRemoveUnique(false);
			} else {
				node.setOrRemoveUnique(true);
				reporter.incrCounter("Brush", "unique_reads", 1);
			}

			output.collect(new Text(node.getNodeId()),
					new Text(node.toNodeMsg()));
		}
	}

	public RunningJob run(String inputPath, String outputPath) throws Exception {
		sLogger.info("Tool name: " + UniqueKmerFilterTagReads.class.getSimpleName() + " [4/5]");
		sLogger.info(" - input: " + inputPath);
		sLogger.info(" - output: " + outputPath);

		JobConf conf = new JobConf(UniqueKmerFilterTagReads.class);
		conf.setJobName(UniqueKmerFilterTagReads.class.getSimpleName() + " " + inputPath);

		Config.initializeConfiguration(conf);

		FileInputFormat.addInputPaths(conf, inputPath);
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(UniqueKmerFilterTagReadsMapper.class);
		conf.setReducerClass(UniqueKmerFilterTagReadsReducer.class);

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
		int res = ToolRunner.run(new Configuration(), new UniqueKmerFilterTagReads(),
				args);
		System.exit(res);
	}
}
