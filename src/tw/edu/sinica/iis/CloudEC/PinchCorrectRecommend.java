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
import java.util.HashMap;
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

public class PinchCorrectRecommend extends Configured implements Tool {
	private static final Logger sLogger = Logger.getLogger(PinchCorrectRecommend.class);

	public static class PinchCorrectRecommendMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		private static int K = 0;

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

			String nodeSEQ = node.getSEQ();
			String nodeQV = node.getQV();

			String nodeIGN = node.getIGN(Utils.IGNP);

			// Here we use a K-mer with a center gap for alignment
			int end = nodeSEQ.length() - (K + 1) + 1;
			for (int i = 0; i < end; i++) {

				// ignore kmers
				if (nodeIGN != null && nodeIGN.charAt(i) == '1') {
					continue;
				}

				String window_f = nodeSEQ.substring(i, i + (K / 2))
						+ nodeSEQ.substring(i + (K / 2) + 1, i + K + 1);
				String window_r = Utils.rcSEQ(window_f);

				if (window_f.contains("N")) {
					continue;
				}

				int middle_pos = i + (K / 2);

				int compare = window_f.compareTo(window_r);
				if (compare < 0) {
					output.collect(
							new Text(Utils.seqEncode(window_f)),
							new Text(node.getNodeId() + "\t" + "f" + "\t"
									+ middle_pos + "\t"
									+ nodeSEQ.charAt(middle_pos) + "\t"
									+ nodeQV.charAt(middle_pos)
									+ "\t" + nodeSEQ.length()));
				} else if (compare > 0) {
					output.collect(
							new Text(Utils.seqEncode(window_r)),
							new Text(node.getNodeId() + "\t" + "r" + "\t"
									+ middle_pos + "\t"
									+ Utils.rcSEQ(nodeSEQ.charAt(middle_pos)) + "\t"
									+ nodeQV.charAt(middle_pos)
									+ "\t" + nodeSEQ.length()));
				}
			}

		}
	}

	public static class PinchCorrectRecommendReducer extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		public class ReadInfo {
			public String id;
			public boolean dir;
			public int pos;
			public short length;
			public byte base;
			public byte qv;

			public ReadInfo(String id1, String dir1, short pos1, String base1,
					String qv1, final short length1) throws IOException {

				id = id1;
				pos = pos1;
				base = base1.getBytes()[0];
				length = length1;

				if (dir1.equals("f")) {
					dir = true;
				} else {
					dir = false;
				}

				qv = Utils.qvValueConvert(qv1, false)[0];
			}
		}

		public class SuggestInfo {
			public StringBuilder[] info;

			public SuggestInfo(final int size) {
				info = new StringBuilder[size];
			}

			public void append(final int pos, final char suggest) {
				if (info[pos] == null) {
					info[pos] = new StringBuilder();
				}
				info[pos].append(suggest);
			}

			public String toString() {
				StringBuilder sb = new StringBuilder();

				for (StringBuilder s : info) {
					if (s != null) {
						sb.append(s.toString());
					}
					sb.append('X');
				}
				sb.deleteCharAt(sb.length() - 1);

				return sb.toString();
			}
		}

		@Override
		public void reduce(Text prefix, Iterator<Text> iter,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			ArrayList<ReadInfo> readlist = new ArrayList<ReadInfo>();
			HashMap<String, SuggestInfo> out_list = new HashMap<String, SuggestInfo>();

			while (iter.hasNext()) {
				// vals: [0]=id, [1]=dir, [2]=pos, [3]=seq, [4]=qv, [5]=length
				String[] vals = iter.next().toString().split("\t", 6);

				ReadInfo readitem = new ReadInfo(vals[0], vals[1],
						Short.parseShort(vals[2]), vals[3], vals[4],
						Short.parseShort(vals[5]));

				readlist.add(readitem);
			}

			// skip large or small stack
			if (readlist.size() < Utils.READ_MIN) {
				readlist.clear();
				return;
			}

			// [0]=A, [1]=T, [2]=C, [3]=G
			int[] qv_sum = new int[4];
			int[] qv_good_cnt = new int[4];
			boolean[] recommends = new boolean[4];

			int baseCnts = 0;

			for (int base = 0; base < qv_sum.length; base++) {
				qv_sum[base] = 0;
				qv_good_cnt[base] = 0;
				recommends[base] = false;
			}

			// summary (A, T, C, G) of each read position
			for (int i = 0; i < readlist.size(); i++) {
				ReadInfo readitem = readlist.get(i);

				int quality_value = readitem.qv;
				int base = Utils.char2idx((char) readitem.base);

				if (base != Utils.char2idx('N')) {
					baseCnts++;

					qv_sum[base] += quality_value;

					if (quality_value >= Utils.QV_GOOD) {
						qv_good_cnt[base]++;
					}
				}
			}

			// skip this position since its has no enough bases
			if (baseCnts < Utils.READ_MIN) {
				readlist.clear();
				return;
			}

			// find out the winner of the position
			int winner_base = 0;

			if (qv_sum[2] > qv_sum[winner_base]) {
				winner_base = 2;
			}
			if (qv_sum[3] > qv_sum[winner_base]) {
				winner_base = 3;
			}
			if (qv_sum[1] > qv_sum[winner_base]) {
				winner_base = 1;
			}

			// winner's qv sum must >= QSUM_WINNER_P
			if (qv_sum[winner_base] < Utils.QSUM_WINNER_P) {
				readlist.clear();
				return;
			}

			// make a replace recommendation for all loser bases iff
			// 1) loser's good qv count <= QV_GOOD_CNT_P
			// 2) loser's qv sum < QRATIO_LOSER_P times of the winner
			for (int base = 0; base < recommends.length; base++) {
				if (base != winner_base) {
					if (qv_good_cnt[base] <= Utils.QV_GOOD_CNT_P
							&& (qv_sum[base] < Utils.QRATIO_LOSER_P
									* qv_sum[winner_base])) {
						recommends[base] = true;
					}
				}
			}

			// make recommendation of each read position
			for (int i = 0; i < readlist.size(); i++) {
				ReadInfo readitem = readlist.get(i);

				int base = Utils.char2idx((char) readitem.base);

				if (base != winner_base) {
					if (base == Utils.char2idx('N') || recommends[base]) {
						char chr = Utils.idx2char(winner_base);

						// rc the base char if reversed
						if (!readitem.dir) {
							chr = Utils.rcSEQ(chr);
						}

						// pos from mapper is always dir f
						SuggestInfo tmp_corrects = out_list.get(readitem.id);
						if (tmp_corrects == null) {
							tmp_corrects = new SuggestInfo(readitem.length);
							out_list.put(readitem.id, tmp_corrects);
						}
						tmp_corrects.append(readitem.pos, chr);

						reporter.incrCounter("Brush", "fix_char", 1);
					}
				}
			}

			readlist.clear();

			for (HashMap.Entry<String, SuggestInfo> entry : out_list.entrySet()) {
				output.collect(new Text(entry.getKey()),
						new Text(Utils.MSGCORRECT + "\t" + Utils
								.corrEncode(entry.getValue().toString())));
			}

			out_list.clear();
		}
	}

	public RunningJob run(String inputPath, String outputPath) throws Exception {
		sLogger.info("Tool name: " + PinchCorrectRecommend.class.getSimpleName() + "  [1/5]");
		sLogger.info(" - input: " + inputPath);
		sLogger.info(" - output: " + outputPath);

		JobConf conf = new JobConf(PinchCorrectRecommend.class);
		conf.setJobName(PinchCorrectRecommend.class.getSimpleName() + " " + inputPath + " " + Config.K);

		Config.initializeConfiguration(conf);

		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(PinchCorrectRecommendMapper.class);
		conf.setReducerClass(PinchCorrectRecommendReducer.class);

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
		int res = ToolRunner.run(new Configuration(), new PinchCorrectRecommend(), args);
		System.exit(res);
	}
}
