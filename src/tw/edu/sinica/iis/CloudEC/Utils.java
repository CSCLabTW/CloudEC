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
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;

public class Utils {
	// Node message codes
	public static final String MSGNODE = "N";
	public static final String MSGUPDATE = "U";
	public static final String MSGCORRECT = "A";
	public static final String MSGIGNP = "P";
	public static final String MSGIGNF = "F";

	// Field codes of a message
	public static final String SEQ = "s";
	public static final String QV = "q";
	public static final String COVERAGE = "v";
	public static final String UNIQUE = "u";
	public static final String IGNP = "p";
	public static final String IGNF = "f";

	// QV boundaries
	public static final short QV_LOW = 0;
	public static final short QV_UP = 40;
	public static final short QV_BASE = 33;
	public static final short QV_GOOD = 20;
	public static final short QV_FIX = QV_BASE;

	public static final short QV_FLATE_FIX = QV_FIX;
	public static final short QV_FLATE_FACTOR = 3;
	public static final short QV_FLATE_UP = QV_UP / QV_FLATE_FACTOR;

	public static final short QV_SMOOTH_RADIOUS = 2;

	// Corrector boundaries
	public static final short QSUM_WINNER_P = 60;
	public static final short QSUM_WINNER_S = 40;

	public static final short QSUM_REPLACE = 60;
	public static final short QSUM_PROTECT = 90;

	public static final float QRATIO_LOSER_P = 0.25f;
	public static final float QRATIO_LOSER_S = 0.25f;

	public static final short QV_GOOD_CNT_P = 1;
	public static final short QV_GOOD_CNT_S = 3;

	// Screening boundaries
	public static final short UNIQUE_READ = 2;

	// Other boundaries
	public static final boolean SKIP_UNDER = true;

	public static final short READ_MIN = 6;
	public static final short BASE_MIN = 1;

	public static final float RATIO_ISPOLY = 0.9f;

	// code bases
	private static final String[] dnachars = { "A", "T", "C", "G", "N" };
	private static final String[] codechars = { "A", "T", "C", "G", "N", "X" };

	private static final char[] hex = { '0', '1', '2', '3', '4', '5', '6', '7',
			'8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };

	private static final String[] h2b = { "0000", "0001", "0010", "0011",
			"0100", "0101", "0110", "0111", "1000", "1001", "1010", "1011",
			"1100", "1101", "1110", "1111" };

	// initialize functions
	private static final HashMap<String, String> str2dna_ = initializeSTR2DNA();
	private static final HashMap<String, String> dna2str_ = initializeDNA2STR();

	private static final HashMap<String, String> str2code_ = initializeSTR2CODE();
	private static final HashMap<String, String> code2str_ = initializeCODE2STR();

	// node members
	private String id;
	private HashMap<String, ArrayList<String>> fields = new HashMap<String, ArrayList<String>>();

	/* Encoder/Decoder lookup table initializer */

	// Encoding table
	// e.g. A=>A, AA=>B, AT=>C, AC=>D, AG=>E, ...
	private static HashMap<String, String> initializeSTR2DNA() {
		int num = 0;
		int asciibase = 'A';

		HashMap<String, String> retval = new HashMap<String, String>();

		for (int xi = 0; xi < dnachars.length; xi++) {
			retval.put(dnachars[xi],
					Character.toString((char) (num + asciibase)));

			num++;

			for (int yi = 0; yi < dnachars.length; yi++) {
				retval.put(dnachars[xi] + dnachars[yi],
						Character.toString((char) (num + asciibase)));
				num++;
			}
		}

		for (int xi = 0; xi < dnachars.length; xi++) {
			for (int yi = 0; yi < dnachars.length; yi++) {
				String m = retval.get(dnachars[xi] + dnachars[yi]);

				for (int zi = 0; zi < dnachars.length; zi++) {
					retval.put(dnachars[xi] + dnachars[yi] + dnachars[zi], m
							+ retval.get(dnachars[zi]));

					for (int wi = 0; wi < dnachars.length; wi++) {
						retval.put(dnachars[xi] + dnachars[yi] + dnachars[zi]
								+ dnachars[wi],
								m + retval.get(dnachars[zi] + dnachars[wi]));

						for (int ui = 0; ui < dnachars.length; ui++) {
							retval.put(dnachars[xi] + dnachars[yi] + dnachars[zi]
									+ dnachars[wi] + dnachars[ui],
									m + retval.get(dnachars[zi] + dnachars[wi])
									+ retval.get(dnachars[ui]));
						}
					}
				}
			}
		}

		return retval;
	}

	// Decoding table
	// e.g. A=>A, B=>AA, C=>AT, D=>AC, E=>AG, ...
	private static HashMap<String, String> initializeDNA2STR() {
		int num = 0;
		int asciibase = 65;

		HashMap<String, String> retval = new HashMap<String, String>();

		for (int xi = 0; xi < dnachars.length; xi++) {
			retval.put(Character.toString((char) (num + asciibase)),
					dnachars[xi]);

			num++;

			for (int yi = 0; yi < dnachars.length; yi++) {
				retval.put(Character.toString((char) (num + asciibase)),
						dnachars[xi] + dnachars[yi]);
				num++;
			}
		}

		return retval;
	}

	private static HashMap<String, String> initializeSTR2CODE() {
		int num = 0;
		int asciibase = 'A';

		HashMap<String, String> retval = new HashMap<String, String>();

		for (int xi = 0; xi < codechars.length; xi++) {
			retval.put(codechars[xi],
					Character.toString((char) (num + asciibase)));

			num++;

			for (int yi = 0; yi < codechars.length; yi++) {
				retval.put(codechars[xi] + codechars[yi],
						Character.toString((char) (num + asciibase)));
				num++;
			}
		}

		for (int xi = 0; xi < codechars.length; xi++) {
			for (int yi = 0; yi < codechars.length; yi++) {
				String m = retval.get(codechars[xi] + codechars[yi]);

				for (int zi = 0; zi < codechars.length; zi++) {
					retval.put(codechars[xi] + codechars[yi] + codechars[zi], m
							+ retval.get(codechars[zi]));

					for (int wi = 0; wi < codechars.length; wi++) {
						retval.put(codechars[xi] + codechars[yi]
								+ codechars[zi] + codechars[wi],
								m + retval.get(codechars[zi] + codechars[wi]));
						for (int ui = 0; ui < codechars.length; ui++) {
							retval.put(
									codechars[xi] + codechars[yi]
											+ codechars[zi] + codechars[wi]
											+ codechars[ui],
									m
											+ retval.get(codechars[zi]
													+ codechars[wi])
											+ retval.get(codechars[ui]));
							for (int vi = 0; vi < codechars.length; vi++) {
								retval.put(
										codechars[xi] + codechars[yi]
												+ codechars[zi] + codechars[wi]
												+ codechars[ui] + codechars[vi],
										m
												+ retval.get(codechars[zi]
														+ codechars[wi])
												+ retval.get(codechars[ui]
														+ codechars[vi]));
							}
						}
					}
				}
			}
		}

		return retval;
	}

	private static HashMap<String, String> initializeCODE2STR() {
		int num = 0;
		int asciibase = 65;

		HashMap<String, String> retval = new HashMap<String, String>();

		for (int xi = 0; xi < codechars.length; xi++) {
			retval.put(Character.toString((char) (num + asciibase)),
					codechars[xi]);

			num++;

			for (int yi = 0; yi < codechars.length; yi++) {
				retval.put(Character.toString((char) (num + asciibase)),
						codechars[xi] + codechars[yi]);
				num++;
			}
		}

		return retval;
	}

	/* Encoder/Decoder helpers */

	// Encode DNA chars as compressed string
	private static String dnaEncode(final String seq) {
		StringBuilder sb = new StringBuilder();

		int l = seq.length();

		int offset = 0;

		while (offset < l) {
			int r = l - offset;

			if (r >= dnachars.length) {
				sb.append(str2dna_.get(seq.substring(offset, offset
						+ dnachars.length)));
				offset += dnachars.length;
			} else {
				sb.append(str2dna_.get(seq.substring(offset, offset + r)));
				offset += r;
			}
		}

		return sb.toString();
	}

	// Decode string (based on A, B, C, D, E, ...) to DNA chars
	private static String dnaDecode(final String encodedSEQ) {
		StringBuilder sb = new StringBuilder();

		for (int i = 0; i < encodedSEQ.length(); i++) {
			sb.append(dna2str_.get(encodedSEQ.substring(i, i + 1)));
		}

		return sb.toString();
	}

	// Encode CODE chars as compressed string
	private static String codeEncode(final String codes) {
		StringBuilder sb = new StringBuilder();

		int l = codes.length();

		int offset = 0;

		while (offset < l) {
			int r = l - offset;

			if (r >= codechars.length) {
				sb.append(str2code_.get(codes.substring(offset, offset
						+ codechars.length)));
				offset += codechars.length;
			} else {
				sb.append(str2code_.get(codes.substring(offset, offset + r)));
				offset += r;
			}
		}

		return sb.toString();
	}

	// Decode string (based on A, B, C, D, E, ...) to CODE chars
	private static String codeDecode(final String encodedCODES) {
		StringBuilder sb = new StringBuilder();

		for (int i = 0; i < encodedCODES.length(); i++) {
			sb.append(code2str_.get(encodedCODES.substring(i, i + 1)));
		}

		return sb.toString();
	}

	public static String seqEncode(final String seq) {
		return dnaEncode(seq);
	}

	public static String seqDecode(final String encodedSEQ) {
		return dnaDecode(encodedSEQ);
	}

	public static boolean seqIsPoly(final String seq, final char base) {
		int cnt = 0;

		for (int i = 0; i < seq.length(); i++) {
			if (seq.charAt(i) == base) {
				cnt++;
			}
		}

		return ((1.0f * cnt / seq.length()) >= RATIO_ISPOLY);
	}

	public static String qvEncode(final String qv) {
		return qv;
	}

	public static String qvDecode(final String encodedQV) {
		return encodedQV;
	}

	public static String corrEncode(final String corrMsg) {
		return codeEncode(corrMsg);
	}

	public static String corrDecode(final String encodedMsg) {
		return codeDecode(encodedMsg);
	}

	/* Converter helpers */

	// Convert input QVs to internal format
	public static String qvInputConvert(final String qv) {
		StringBuilder sb = new StringBuilder(qv);

		for (int i = 0; i < sb.length(); i++) {
			if (sb.charAt(i) - QV_BASE < QV_LOW) {
				sb.setCharAt(i, (char) (QV_BASE + QV_LOW));
			} else if (sb.charAt(i) - QV_BASE > QV_UP) {
				sb.setCharAt(i, (char) (QV_BASE + QV_UP));
			}
		}

		return sb.toString();
	}

	// Convert ASCII based QVs into values (as byte array)
	public static byte[] qvValueConvert(final String asciiQV,
			final boolean reverse) {
		byte[] qv_int = new byte[asciiQV.length()];

		for (int i = 0; i < qv_int.length; i++) {
			int target = i;

			if (reverse) {
				target = qv_int.length - 1 - i;
			}

			qv_int[i] = (byte) (asciiQV.charAt(target) - QV_BASE);
		}

		return qv_int;
	}

	// Convert internal QV encoding to output format
	public static String qvOutputConvert(final String qv) {
		return qvInflate(qvDeflate(qv));
	}

	// Covert IGN string into compressed HEX string
	// e.g. str{1, 1, 1, 1}=16 => 'F'
	public static String str2hex(final String ignorelist) {
		StringBuilder sb = new StringBuilder();

		byte b = 0x00;
		for (int i = 0; i < ignorelist.length(); i++) {
			b <<= 1;

			if (ignorelist.charAt(i) == '1') {
				b |= 0x01;
			}

			if ((i + 1) % 4 == 0) {
				sb.append(hex[b]);
				b = 0x00;
			}
		}

		return sb.toString();
	}

	public static char idx2char(final int base) {
		return dnachars[base].charAt(0);
	}

	public static int char2idx(final char base) {
		for (int i = 0; i < dnachars.length; i++) {
			if (base == dnachars[i].charAt(0)) {
				return i;
			}
		}

		return (dnachars.length - 1);
	}

	// deflate 8-bit continuous QV to 4-bit continuous value with offset
	public static String qvDeflate(final String qv) {
		StringBuilder sb = new StringBuilder(qv);

		for (int i = 0; i < sb.length(); i++) {
			int val = Integer.valueOf(sb.charAt(i) - QV_BASE);

			sb.setCharAt(i, (char) (Math.min(val / QV_FLATE_FACTOR, QV_FLATE_UP) + QV_BASE));
		}

		return sb.toString();
	}

	// inflate 4-bit continuous QV to 8-bit sparse value with offset
	public static String qvInflate(final String qv) {
		StringBuilder sb = new StringBuilder(qv);

		for (int i = 0; i < sb.length(); i++) {
			int val = Integer.valueOf(sb.charAt(i) - QV_BASE);

			sb.setCharAt(i, (char) ((val * QV_FLATE_FACTOR + (QV_FLATE_FACTOR / 2)) + QV_BASE));
		}

		return sb.toString();
	}

	// Smooth QV according to its neighbors' minimal value
	public static String qvSmooth(final String qv) {
		StringBuilder sb = new StringBuilder(qv);

		for (int i = 0; i < sb.length(); i++) {
			int start = Math.max(i - QV_SMOOTH_RADIOUS, 0);
			int end = Math.min(i + QV_SMOOTH_RADIOUS, qv.length() - 1);

			int min = Integer.valueOf(qv.charAt(i) - QV_BASE);
			for (int j = start; j <= end; j++) {
				min = Math.min(min, Integer.valueOf(qv.charAt(j) - QV_BASE));
			}

			sb.setCharAt(i, (char) (min + QV_BASE));
		}

		return sb.toString();
	}

	/* helper functions */

	public String getNodeId() {
		return id;
	}

	public void setNodeId(final String nid) {
		id = nid;
	}

	private ArrayList<String> getOrAddField(final String field) {
		if (fields.containsKey(field)) {
			return fields.get(field);
		}

		ArrayList<String> retval = new ArrayList<String>();
		fields.put(field, retval);

		return retval;
	}

	public boolean containsField(final String key) {
		return fields.containsKey(key);
	}

	public void setField(final String key, final String value) {
		ArrayList<String> l = getOrAddField(key);
		l.clear();
		l.add(value);
	}

	public void addField(final String key, final String value) {
		ArrayList<String> l = getOrAddField(key);
		l.add(value);
	}

	public ArrayList<String> getField(final String key) {
		return fields.get(key);
	}

	public void removeField(final String key) {
		fields.remove(key);
	}

	public float getCoverage() {
		return Float.parseFloat(fields.get(COVERAGE).get(0));
	}

	public void setCoverage(final float cov) {
		ArrayList<String> l = getOrAddField(COVERAGE);
		l.clear();
		l.add(Float.toString(cov));
	}

	public String getSEQ() {
		return getSEQ(true);
	}

	public String getSEQ(final boolean decode) {
		if (decode) {
			return seqDecode(fields.get(SEQ).get(0));
		} else {
			return fields.get(SEQ).get(0);
		}
	}

	public void setSEQ(final String seq) {
		ArrayList<String> l = getOrAddField(SEQ);
		l.clear();
		l.add(seqEncode(seq));
	}

	public String getQV() {
		return getQV(true);
	}

	public String getQV(final boolean decode) {
		if (decode) {
			return qvDecode(fields.get(QV).get(0));
		} else {
			return fields.get(QV).get(0);
		}
	}

	public void setQV(final String qv) {
		ArrayList<String> l = getOrAddField(QV);
		l.clear();
		l.add(qvEncode(qv));
	}

	public void removeQV() {
		fields.remove(QV);
	}

	public String getIGN(final String IGNType) {
		String ignoreListHex = null;

		if (fields.containsKey(IGNType)) {
			ignoreListHex = fields.get(IGNType).get(0);

			StringBuilder sb = new StringBuilder();

			for (int i = 0; i < ignoreListHex.length(); i++) {
				int c = ignoreListHex.charAt(i) - '0';
				if (c > 10) {
					c = c - 7;
				}

				sb.append(h2b[c]);
			}

			return sb.toString();
		}

		return null;
	}

	public void setIGN(final String IGNType, final String ign) {
		ArrayList<String> l = getOrAddField(IGNType);
		l.clear();
		l.add(ign);
	}

	public void removeIGN(final String IGNType) {
		fields.remove(IGNType);
	}

	public int getLen() {
		return getSEQ().length();
	}

	public int getGCCnt() {
		String seq = getSEQ();

		int gccnt = 0;
		for (int i = 0; i < seq.length(); i++) {
			if (seq.charAt(i) == 'C' || seq.charAt(i) == 'G') {
				gccnt++;
			}
		}

		return gccnt;
	}

	public boolean isUnique() {
		if (fields.containsKey(UNIQUE)) {
			return fields.get(UNIQUE).get(0).equals("1");
		}

		return false;
	}

	public void setOrRemoveUnique(final boolean unique) {
		if (unique) {
			ArrayList<String> l = getOrAddField(UNIQUE);
			l.clear();
			l.add("1");
		} else {
			fields.remove(UNIQUE);
		}
	}

	public String toNodeMsg() {
		return toNodeMsg(false);
	}

	public String toNodeMsg(final boolean outputNodeID) {
		StringBuilder sb = new StringBuilder();

		DecimalFormat df = new DecimalFormat("0.00");

		if (outputNodeID) {
			sb.append(id);
			sb.append("\t");
		}

		sb.append(MSGNODE);

		sb.append("\t*");
		sb.append(SEQ);
		sb.append("\t");
		sb.append(getSEQ(false));

		if (containsField(QV)) {
			sb.append("\t*");
			sb.append(QV);
			sb.append("\t");
			sb.append(getQV(false));
		}

		sb.append("\t*");
		sb.append(COVERAGE);
		sb.append("\t");
		sb.append(df.format(getCoverage()));

		if (containsField(UNIQUE)) {
			sb.append("\t*");
			sb.append(UNIQUE);
			sb.append("\t");
			sb.append(fields.get(UNIQUE).get(0));
		}

		if (containsField(IGNF)) {
			sb.append("\t*");
			sb.append(IGNF);
			for (String t : fields.get(IGNF)) {
				sb.append("\t");
				sb.append(t);
			}
		}

		if (containsField(IGNP)) {
			sb.append("\t*");
			sb.append(IGNP);
			for (String t : fields.get(IGNP)) {
				sb.append("\t");
				sb.append(t);
			}
		}

		return sb.toString();
	}

	public void fromNodeMsg(final String nodestr) throws IOException {
		fields.clear();

		String[] items = nodestr.split("\t");

		id = items[0];
		parseNodeMsg(items, 1);
	}

	public void parseNodeMsg(final String[] items, int offset)
			throws IOException {
		if (!items[offset].equals(MSGNODE)) {
			throw new IOException("Unknown code: " + items[offset]);
		}

		ArrayList<String> l = null;

		offset++;

		while (offset < items.length) {
			if (items[offset].charAt(0) == '*' && items[offset].length() == 2) {
				String type = items[offset].substring(1);
				l = fields.get(type);

				if (l == null) {
					l = new ArrayList<String>();
					fields.put(type, l);
				}
			} else if (l != null) {
				l.add(items[offset]);
			}

			offset++;
		}
	}

	// reverse complement
	public static Character rcSEQ(final char chr) {
		return rcSEQ(String.valueOf(chr)).charAt(0);
	}

	public static String rcSEQ(final String seq) {
		StringBuilder sb = new StringBuilder();

		for (int i = seq.length() - 1; i >= 0; i--) {
			if (seq.charAt(i) == 'A') {
				sb.append('T');
			} else if (seq.charAt(i) == 'T') {
				sb.append('A');
			} else if (seq.charAt(i) == 'C') {
				sb.append('G');
			} else if (seq.charAt(i) == 'G') {
				sb.append('C');
			} else {
				sb.append(seq.charAt(i));
			}
		}

		return sb.toString();
	}

	public Utils(final String nid) {
		id = nid;
	}

	public Utils() {
	}

	public static void main(String[] args) throws Exception {
	}
}
