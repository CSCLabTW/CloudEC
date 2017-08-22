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

public class SpreadCorrectRecommend extends Configured implements Tool {
	private static final Logger sLogger = Logger.getLogger(SpreadCorrectRecommend.class);

	public static class SpreadCorrectRecommendMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		public static int K = 0;

		public static String SCHEME = null;
		public static int ARM = 0;
		public static int HEIGHT = 0;

		@Override
		public void configure(JobConf job) {
			K = job.getInt("K", 0);

			SCHEME = job.get("ARM_SCHEME", null);
			ARM = job.getInt("ARM_L", 0);
			HEIGHT = job.getInt("ARM_H", 0);
		}

		@Override
		public void map(LongWritable lineid, Text nodetxt,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			Utils node = new Utils();
			node.fromNodeMsg(nodetxt.toString());

			String nodeID = node.getNodeId();
			String nodeSEQ = node.getSEQ();
			String nodeQV = Utils.qvSmooth(Utils.qvDeflate(node.getQV()));

			String nodeIGN = node.getIGN(Utils.IGNF);

			// Here we use a Kmer for alignment
			int end = nodeSEQ.length() - K + 1;
			for (int i = 0; i < end; i++) {
				// ignore kmers
				if (nodeIGN != null && nodeIGN.charAt(i) == '1') {
					continue;
				}

				String kmer_f = nodeSEQ.substring(i, i + K);
				String kmer_r = Utils.rcSEQ(kmer_f);

				if (kmer_f.contains("N")) {
					continue;
				}

				int wing_pos_left = 0;
				int wing_pos_right = nodeSEQ.length();

				if (ARM != -1) {
					if (SCHEME != null && SCHEME.equals("CLA")) {
						if (i < ARM) {
							wing_pos_left = Math.max(0, i - ARM);
							wing_pos_right = Math.min(i + K + (2 * ARM - i),
									nodeSEQ.length());
						} else if (nodeSEQ.length() - (i + K) < ARM) {
							wing_pos_right = Math.min(i + K + ARM,
									nodeSEQ.length());
							wing_pos_left = Math.max(0,
									i - (2 * ARM - (wing_pos_right - (i + K))));
						} else {
							wing_pos_left = i - ARM;
							wing_pos_right = i + K + ARM;
						}
					} else if (SCHEME != null && SCHEME.equals("ENV")) {
						/* for left */
						if (i < K + ARM) {
							wing_pos_left = Math.max(0, i - ARM);
						} else {
							wing_pos_left = K + (int) Math.floor(
									1.0 * (i - (K + ARM) + 1) * (end - K - 1)
											/ (end - (K + ARM) + 1));
						}

						/* for right */
						if (i < end - (K + ARM)) {
							wing_pos_right = K + (int) Math.floor(1.0 * (i + 1)
									* (end - K - 1) / (end - (K + ARM) + 1));
						} else {
							wing_pos_right = Math.min(i + K + ARM,
									nodeSEQ.length());

						}
					} else if (SCHEME != null && SCHEME.equals("GNV")) {
						int PtA = ARM + HEIGHT;
						int PtD = ARM;
						int PtB = ARM + (int) Math
								.ceil(1.0 * end * (PtA - PtD) / end);
						int PtC = ARM + (int) Math
								.ceil(1.0 * K * (PtA - PtD) / nodeSEQ.length());

						if (i < PtA + 1) {
							// wing_pos_left = 0;
						} else if (i >= PtA + 1 && i < K + PtB) {
							wing_pos_left = 0 + (int) Math
									.floor(1.0 * (i - (PtA + 1) + 1) * (K - 1)
											/ ((K + PtB) - (PtA + 1)));
						} else {
							wing_pos_left = K + (int) Math.floor(
									1.0 * (i - (K + PtB) + 1) * (end - K - 1)
											/ (end - (K + PtB) + 1));
						}

						if (i < end - (K + PtC)) {
							wing_pos_right = K + (int) Math.floor(1.0 * (i + 1)
									* (end - K - 1) / (end - (K + PtC)));
						} else if (i >= end - (K + PtC) && i < end - ARM - 1) {
							wing_pos_right = (end - 1) + (int) Math.floor(1.0
									* (i - (end - (K + PtC)) + 1) * (K - 1)
									/ ((end - ARM - 1) - (end - (K + PtC))));
						} else {
							// wing_pos_right = nodeSEQ.length();
						}
					} else {
						wing_pos_left = Math.max(0, i - ARM);
						wing_pos_right = Math.min(i + K + ARM,
								nodeSEQ.length());
					}
				}

				int compare = kmer_f.compareTo(kmer_r);
				if (compare < 0) {
					output.collect(
							new Text(Utils.seqEncode(kmer_f)),
							new Text(nodeID
									+ "\t"
									+ Utils.seqEncode(nodeSEQ.substring(
											wing_pos_left, i))
									+ "\t"
									+ Utils.seqEncode(nodeSEQ.substring(i
											+ K, wing_pos_right))
									+ "\t"
									+ Utils.qvEncode(nodeQV.substring(
											wing_pos_left, wing_pos_right))
									+ "\t" + "f" + "\t" + i + "\t"
									+ wing_pos_left + "\t"
									+ nodeSEQ.length()));
				} else if (compare > 0) {
					output.collect(
							new Text(Utils.seqEncode(kmer_r)),
							new Text(nodeID
									+ "\t"
									+ Utils.seqEncode(nodeSEQ.substring(
											wing_pos_left, i))
									+ "\t"
									+ Utils.seqEncode(nodeSEQ.substring(i
											+ K, wing_pos_right))
									+ "\t"
									+ Utils.qvEncode(nodeQV.substring(
											wing_pos_left, wing_pos_right))
									+ "\t" + "r" + "\t" + (end - i - 1)
									+ "\t"
									+ (nodeSEQ.length() - wing_pos_right)
									+ "\t" + nodeSEQ.length()));
				}
			}
		}
	}

	public static class SpreadCorrectRecommendReducer extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		private static int K = 0;

		public static int StackMax = 0;
		public static int StackMin = 0;

		@Override
		public void configure(JobConf job) {
			K = job.getInt("K", 0);

			StackMax = job.getInt("STACK_MAX", 0);
			StackMin = job.getInt("STACK_MIN", 0);
		}

		public class ReadInfo {
			public String id;
			public boolean dir;
			public int pos;
			public short offset;
			public short length;
			public byte[] seq;
			public byte[] qv_int;

			public ReadInfo(final String id1, final String dir1,
					final short pos1, final String seq_p1, final String seq_k1,
					final String seq_s1, final String qv1, final short offset1,
					final short length1) throws IOException {

				id = id1;
				pos = pos1;
				offset = offset1;
				length = length1;

				if (dir1.equals("f")) {
					dir = true;

					seq = (Utils.seqDecode(seq_p1) + Utils.seqDecode(seq_k1) + Utils
							.seqDecode(seq_s1)).getBytes();
					qv_int = Utils.qvValueConvert(Utils.qvInflate(Utils.qvDecode(qv1)), false);
				} else {
					dir = false;

					seq = (Utils.rcSEQ(Utils.seqDecode(seq_s1))
							+ Utils.seqDecode(seq_k1) + Utils.rcSEQ(Utils
							.seqDecode(seq_p1))).getBytes();
					qv_int = Utils.qvValueConvert(Utils.qvInflate(Utils.qvDecode(qv1)), true);
				}
			}

			public int getARMLeft() {
				return (pos - offset);
			}

			public int getARMRight() {
				return (seq.length - K - (pos - offset));
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

		private boolean makeColEC(final ArrayList<ReadInfo> readlist,
				final int colbias, final boolean under_kmer,
				HashMap<String, SuggestInfo> out_list, Reporter reporter) {

			// [0]=A, [1]=T, [2]=C, [3]=G
			int[] qv_sum = new int[4];
			int[] qv_good_cnt = new int[4];
			boolean[] recommends = new boolean[4];

			for (int base = 0; base < qv_sum.length; base++) {
				qv_sum[base] = 0;
				qv_good_cnt[base] = 0;
				recommends[base] = false;
			}

			int baseCnts = 0;

			// summary (A, T, C, G) of each read position
			for (int i = 0; i < readlist.size(); i++) {
				ReadInfo readitem = readlist.get(i);

				int pos = readitem.pos + colbias - readitem.offset;

				if (pos < 0 || pos > readitem.seq.length - 1) {
					continue;
				}

				int quality_value = readitem.qv_int[pos];
				int base = Utils.char2idx((char) readitem.seq[pos]);

				if (base != Utils.char2idx('N')) {
					baseCnts++;

					qv_sum[base] += quality_value;

					if (quality_value >= Utils.QV_GOOD) {
						qv_good_cnt[base]++;
					}
				}
			}

			// skip this position since it has no enough bases
			if (baseCnts < Utils.BASE_MIN) {
				return false;
			}

			// find out the winner of the position
			int base_winner = 0;

			if (qv_sum[2] > qv_sum[base_winner]) {
				base_winner = 2;
			}
			if (qv_sum[3] > qv_sum[base_winner]) {
				base_winner = 3;
			}
			if (qv_sum[1] > qv_sum[base_winner]) {
				base_winner = 1;
			}

			// winner's qv sum must >= QSUM_WINNER_S
			if (qv_sum[base_winner] < Utils.QSUM_WINNER_S) {
				return false;
			}

			// the maximum value of non-winner bases
			int qv_sum_max_loser = 0;

			for (int base = 0; base < qv_sum.length; base++) {
				if (base != base_winner) {
					if (qv_sum_max_loser < qv_sum[base]) {
						qv_sum_max_loser = qv_sum[base];
					}
				}
			}

			// make a replace recommendation for all loser bases iff
			// 1) losers' qv sum ARE all <= QRATIO_LOSER_S times of the winner
			// 2) loser is not corrected, and its qv sum < QSUM_REPLACE
			if (qv_sum_max_loser <= Utils.QRATIO_LOSER_S * qv_sum[base_winner]) {
				for (int base = 0; base < recommends.length; base++) {
					if (base != base_winner) {
						if (qv_sum[base] != 0
								&& qv_sum[base] < Utils.QSUM_REPLACE) {
							recommends[base] = true;
						}
					}
				}
			}

			// make a protect recommendation for the winner iff
			// 1) loser's qv sum are ALL < QSUM_REPLACE
			// 2) winner's qv sum >= QSUM_REPLACE if under the kmer range
			// 3) winner's qv sum >= QSUM_PROTECT,
			// or winner's good qv count is at least QV_GOOD_CNT_S
			if (qv_sum_max_loser < Utils.QSUM_REPLACE) {
				if (!under_kmer || qv_sum[base_winner] >= Utils.QSUM_REPLACE) {
					if (qv_sum[base_winner] >= Utils.QSUM_PROTECT
							|| qv_good_cnt[base_winner] >= Utils.QV_GOOD_CNT_S) {
						recommends[base_winner] = true;
					}
				}
			}

			// make recommendation of each read position
			for (int i = 0; i < readlist.size(); i++) {
				ReadInfo readitem = readlist.get(i);

				int pos = readitem.pos + colbias - readitem.offset;

				if (pos < 0 || pos > readitem.seq.length - 1) {
					continue;
				}

				int base = Utils.char2idx((char) readitem.seq[pos]);

				if (base == base_winner) {
					if (recommends[base]) {
						pos = pos + readitem.offset;

						if (!readitem.dir) {
							pos = readitem.length - 1 - pos;
						}

						SuggestInfo tmp_corrects = out_list.get(readitem.id);
						if (tmp_corrects == null) {
							tmp_corrects = new SuggestInfo(readitem.length);
							out_list.put(readitem.id, tmp_corrects);
						}
						tmp_corrects.append(pos, 'N');

						reporter.incrCounter("Brush", "confirm_char", 1);
					}
				} else {
					if (base == Utils.char2idx('N') || recommends[base]) {
						char chr = Utils.idx2char(base_winner);

						pos = pos + readitem.offset;

						if (!readitem.dir) {
							chr = Utils.rcSEQ(chr + "").charAt(0);
							pos = readitem.length - 1 - pos;
						}

						SuggestInfo tmp_corrects = out_list.get(readitem.id);
						if (tmp_corrects == null) {
							tmp_corrects = new SuggestInfo(readitem.length);
							out_list.put(readitem.id, tmp_corrects);
						}
						tmp_corrects.append(pos, chr);

						reporter.incrCounter("Brush", "fix_char", 1);
					}
				}
			}

			// skip the rest positions because of a branch
			for (int base = 0; base < qv_sum.length; base++) {
				if (base != base_winner) {
					if (qv_sum[base] >= Utils.QSUM_REPLACE) {
						return true;
					}
				}
			}

			return false;
		}

		@Override
		public void reduce(Text prefix, Iterator<Text> iter,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			ArrayList<ReadInfo> readlist = new ArrayList<ReadInfo>();
			HashMap<String, SuggestInfo> out_list = new HashMap<String, SuggestInfo>();

			int armLeft = 0;
			int armRight = 0;

			while (iter.hasNext()) {
				// vals: [0]=id, [1]=seq_p, [2]=seq_s [3]=qv, [4]=dir, [5]=pos,
				// [6]=offset, [7]=length
				String[] vals = iter.next().toString().split("\t", 8);

				ReadInfo readitem = new ReadInfo(vals[0], vals[4],
						Short.parseShort(vals[5]), vals[1], prefix.toString(),
						vals[2], vals[3], Short.parseShort(vals[6]),
						Short.parseShort(vals[7]));

				armLeft = Math.max(armLeft, readitem.getARMLeft());
				armRight = Math.max(armRight, readitem.getARMRight());

				readlist.add(readitem);
			}

			// skip large or small stack, -1 ignore
			if ((StackMax != -1 && readlist.size() > StackMax)
					|| (StackMin != -1 && readlist.size() < StackMin)) {
				readlist.clear();
				return;
			}

			// left range
			for (int j = armLeft - 1; j >= 0; j--) {
				boolean branch = false;
				branch = makeColEC(readlist, (-armLeft + j), false, out_list, reporter);
				if (branch) {
					break;
				}
			}

			// K range
			if (!Utils.SKIP_UNDER) {
				for (int j = 0; j < K; j++) {
					makeColEC(readlist, j, true, out_list, reporter);
				}
			}

			// right range
			for (int j = 0; j < armRight; j++) {
				boolean branch = false;
				branch = makeColEC(readlist, (j + K), false, out_list, reporter);
				if (branch) {
					break;
				}
			}

			readlist.clear();

			// output correction message
			for (HashMap.Entry<String, SuggestInfo> entry : out_list.entrySet()) {
				output.collect(new Text(entry.getKey()),
						new Text(Utils.MSGCORRECT + "\t" + Utils
								.corrEncode(entry.getValue().toString())));
			}

			out_list.clear();
		}
	}

	public RunningJob run(String inputPath, String outputPath) throws Exception {
		sLogger.info("Tool name: " + SpreadCorrectRecommend.class.getSimpleName() + " [3/5]");
		sLogger.info(" - input: " + inputPath);
		sLogger.info(" - output: " + outputPath);

		JobConf conf = new JobConf(SpreadCorrectRecommend.class);

		conf.setJobName(SpreadCorrectRecommend.class.getSimpleName() + " " + inputPath + " " + Config.K);

		Config.initializeConfiguration(conf);

		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(SpreadCorrectRecommendMapper.class);
		conf.setReducerClass(SpreadCorrectRecommendReducer.class);

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
		int res = ToolRunner.run(new Configuration(), new SpreadCorrectRecommend(), args);
		System.exit(res);
	}
}
