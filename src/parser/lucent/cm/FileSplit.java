package parser.lucent.cm;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import util.LogMgr;

/**
 * 将W网阿朗参数文件（XML格式），分割，仅保留一个或多个RNC的数据。
 * 
 * @author ChenSijiang 2011-3-17 上午09:02:10
 */
final class FileSplit {

	private static final Logger logger = LogMgr.getInstance().getSystemLogger();

	/**
	 * 开始分割，失败返回<code>null</code>.
	 * 
	 * @param file
	 *            要分割的文件。
	 * @param rnc
	 *            要保留的一个或多个RNC.
	 * @return 分割后的文件。
	 */
	public static File split(File file, List<String> rnc) {
		if (file == null || rnc == null || rnc.size() == 0)
			return null;

		File result = null;

		InputStream in = null;
		Reader r = null;
		BufferedReader br = null;

		PrintWriter pw = null;

		try {
			in = new FileInputStream(file);
			r = new InputStreamReader(in);
			br = new BufferedReader(r);

			result = new File(file.getAbsoluteFile() + ".tmp." + System.currentTimeMillis());
			pw = new PrintWriter(result);

			boolean reserv = false;
			boolean findRnc = false;
			String line = null;
			int ccc = 0;
			while ((line = br.readLine()) != null) {
				ccc++;
				if (ccc % 1000000 == 0) {
					logger.debug(file.getAbsoluteFile() + " 已经分析" + ccc + "行");
				}
				line = line.replace("</snapshot>", "");
				String trimedLine = line.trim();
				if (trimedLine.length() == 0)
					continue;

				String lowerLine = trimedLine.toLowerCase();

				if (lowerLine.startsWith("<rnc id=\"")) {
					findRnc = true;
					for (String rncName : rnc) {
						if (lowerLine.startsWith("<rnc id=\"" + rncName.toLowerCase() + "\"")) {
							reserv = true;
							break;
						}
					}
				} else if (lowerLine.endsWith("</rnc>")) {
					if (reserv) {
						pw.println(line);
						pw.flush();
						reserv = false;
						findRnc = false;
						continue;
					} else {
						reserv = false;
						findRnc = false;
						continue;
					}
				}

				if (!findRnc || reserv) {
					pw.println(line);
					pw.flush();
				}

			}
			logger.debug(file.getAbsoluteFile() + " 总计" + ccc + "行");
		} catch (Exception e) {
			logger.error("分割阿朗参数文件时出错 - " + file.getAbsolutePath(), e);
			return null;
		} finally {
			if (pw != null) {
				pw.println("</snapshot>");
				pw.flush();
				pw.close();
			}
			IOUtils.closeQuietly(pw);
			IOUtils.closeQuietly(br);
			IOUtils.closeQuietly(r);
			IOUtils.closeQuietly(in);
			try {
				file.delete();
				result.renameTo(file);
			} catch (Exception unused) {
			}
		}

		return file;
	}

	/**
	 * 将一个原始文件拆分成多个，每个RNC一个文件。 注意：每RNC的<RNC>节点都有两个。 例如，下面这个是需要的：
	 * 
	 * <pre>
	 * <xmp>
	 * <RNC id="WEIJINGNANLUrnc3" version="07_01_00" model="RNC" clusterId="UMAINSERV1">
	 * <RNCEquipment id="0">
	 * ....
	 * </xmp>
	 * </pre>
	 * 
	 * 下面这个，也是需要的：
	 * 
	 * <pre>
	 * <xmp>
	 * <RNC id="WEIJINGNANLUrnc3" version="07_01_00" model="RNC" clusterId="UMAINSERV1">
	 * <attributes>
	 * ....
	 * </xmp>
	 * </pre>
	 */
	public static List<File> spliteAllRnc2(File src) {
		if (src == null || !src.isFile())
			return null;

		List<File> results = new ArrayList<File>();

		Map<String, RNCWriter> writers = new HashMap<String, RNCWriter>();

		InputStream in = null;
		Reader reader = null;
		BufferedReader br = null;
		try {
			in = new FileInputStream(src);
			reader = new InputStreamReader(in);
			br = new BufferedReader(reader);
			String line = null;
			boolean findRNCElement = false;
			String lastRncName = null;
			logger.debug("开始处理阿朗参数文件 - " + src.getAbsolutePath());
			while ((line = br.readLine()) != null) {
				if (findRNCElement) {
					RNCWriter pw = writers.get(lastRncName);
					if (line.contains("</RNC>")) {
						line = line.substring(0, line.indexOf("</RNC>")) + "</RNC>";
						findRNCElement = false;
						pw.println(line);
						pw.flush();
					} else {
						pw.println(line);
						pw.flush();
					}
				} else if (line.contains("<RNC id=")) {
					findRNCElement = true;
					line = line.substring(line.indexOf("<RNC id="));
					String rncName = findRNCName(line);
					lastRncName = rncName;
					RNCWriter pw = null;
					if (!writers.containsKey(rncName)) {
						pw = new RNCWriter(new File(src.getAbsolutePath() + "." + rncName));
						pw.println("<uway>");
						writers.put(rncName, pw);
					} else {
						pw = writers.get(rncName);
					}
					pw.println(line);
					pw.flush();
				}

			}
		} catch (Exception e) {
			logger.error("处理阿朗参数时发生异常：" + src.getAbsolutePath(), e);
		} finally {
			Collection<RNCWriter> vals = writers.values();
			for (RNCWriter w : vals) {
				results.add(w.file);
				w.println("</uway>");
				w.flush();
				IOUtils.closeQuietly(w);
			}

			IOUtils.closeQuietly(br);
			IOUtils.closeQuietly(reader);
			IOUtils.closeQuietly(in);
			if (!src.delete())
				logger.warn("文件删除失败 - " + src.getAbsolutePath());
			logger.debug("阿朗参数文件处理完毕 - " + src.getAbsolutePath() + "，文件列表：" + results);
		}

		return results;
	}

	/**
	 * 将一个原始文件拆分成多个，每个RNC一个文件。 注意：每RNC的<RNC>节点都有两个，只取下面有<RNCEquipment>节点的那一个。 例如，下面这个是需要的：
	 * 
	 * <pre>
	 * <xmp>
	 * <RNC id="WEIJINGNANLUrnc3" version="07_01_00" model="RNC" clusterId="UMAINSERV1">
	 * <RNCEquipment id="0">
	 * ....
	 * </xmp>
	 * </pre>
	 * 
	 * 下面这个，是不需要的：
	 * 
	 * <pre>
	 * <xmp>
	 * <RNC id="WEIJINGNANLUrnc3" version="07_01_00" model="RNC" clusterId="UMAINSERV1">
	 * <attributes>
	 * ....
	 * </xmp>
	 * </pre>
	 */
	public static List<File> spliteAllRnc(File src) {
		if (src == null || !src.isFile())
			return null;

		int count = 0;
		List<File> results = new ArrayList<File>();

		InputStream in = null;
		Reader reader = null;
		BufferedReader br = null;
		try {
			in = new FileInputStream(src);
			reader = new InputStreamReader(in);
			br = new BufferedReader(reader);
			String line = null;
			boolean findRNCElement = false;
			File dest = null;
			PrintWriter out = null;
			logger.debug("开始处理阿朗参数文件 - " + src.getAbsolutePath());
			while ((line = br.readLine()) != null) {
				if (findRNCElement) {
					if (line.contains("</RNC>")) {
						line = line.substring(0, line.indexOf("</RNC>")) + "</RNC>";
						out.println(line);
						out.flush();
						IOUtils.closeQuietly(out);
						findRNCElement = false;
						results.add(dest);
						logger.debug("已提取出第" + (count) + "个RNC的阿朗参数文件 - " + dest.getAbsolutePath());
						dest = null;
						out = null;
					} else {
						out.println(line);
						out.flush();
					}
				} else if (line.contains("<RNC id=")) {
					String nextLine = br.readLine();
					if (nextLine.contains("<RNCEquipment ") || line.contains("<RNCEquipment ")) {
						findRNCElement = true;
						count++;
						dest = new File(src.getAbsolutePath() + "." + count);
						out = new PrintWriter(dest);
						line = line.substring(line.indexOf("<RNC id="));
						out.println(line);
						out.println(nextLine);
						out.flush();
					}
				}

			}
		} catch (Exception e) {
			logger.error("处理阿朗参数时发生异常：" + src.getAbsolutePath(), e);
		} finally {
			IOUtils.closeQuietly(br);
			IOUtils.closeQuietly(reader);
			IOUtils.closeQuietly(in);
			if (!src.delete())
				logger.warn("文件删除失败 - " + src.getAbsolutePath());
			logger.debug("阿朗参数文件处理完毕 - " + src.getAbsolutePath());
		}

		return results;
	}

	public static List<File> splitFull(File file) {
		logger.debug("开始拆分阿朗参数文件 - " + file);
		if (file == null || !file.exists())
			return null;

		File dir = new File(file.getAbsolutePath() + "_split" + File.separator);
		dir.mkdirs();

		Map<String, RNCWriter> writers = new HashMap<String, RNCWriter>();

		int rncIndex = 0;
		int btsIndex = 0;
		int btsCount = 0;

		List<File> results = new ArrayList<File>();

		InputStream in = null;
		Reader reader = null;
		BufferedReader br = null;
		String currElementName = null;
		try {
			in = new FileInputStream(file);
			reader = new InputStreamReader(in);
			br = new BufferedReader(reader);
			String line = null;
			String rRemain = null;
			while ((line = br.readLine()) != null) {
				if (rRemain != null) {
					line = rRemain + line;
					rRemain = null;
				}

				if (line.contains("<RNC id=")) {
					currElementName = "RNC_" + (rncIndex++);
					writers.put(currElementName, new RNCWriter(new File(dir, currElementName + ".xml")));
					String right = line.substring(line.indexOf("<RNC id="));
					writers.get(currElementName).println("<part>");
					writers.get(currElementName).println(right);
				} else if (line.contains("<Cluster id=")) {
					currElementName = "Cluster";
					if (!writers.containsKey(currElementName)) {
						writers.put(currElementName, new RNCWriter(new File(dir, currElementName + ".xml")));
						writers.get(currElementName).println("<part>");
					}

					String right = line.substring(line.indexOf("<Cluster id="));
					writers.get(currElementName).println(right);

				} else if (line.contains("<BTSEquipment id=")) {
					btsCount++;
					if (btsCount % 200 == 0) {
						btsIndex++;
						currElementName = "BTSEquipment_" + (btsIndex);
						writers.put(currElementName, new RNCWriter(new File(dir, currElementName + ".xml")));
						String right = line.substring(line.indexOf("<BTSEquipment id="));
						writers.get(currElementName).println("<part>");
						writers.get(currElementName).println(right);
					} else {
						currElementName = "BTSEquipment_" + (btsIndex);
						if (!writers.containsKey(currElementName)) {

							writers.put(currElementName, new RNCWriter(new File(dir, currElementName + ".xml")));
							writers.get(currElementName).println("<part>");
						}

						String right = line.substring(line.indexOf("<BTSEquipment id="));
						writers.get(currElementName).println(right);
					}
				} else if (line.contains("<Site id=")) {
					currElementName = "Site";
					if (!writers.containsKey(currElementName)) {
						writers.put(currElementName, new RNCWriter(new File(dir, currElementName + ".xml")));
						writers.get(currElementName).println("<part>");
					}
					String right = line.substring(line.indexOf("<Site id="));
					writers.get(currElementName).println(right);
				} else if (line.contains("<PMCluster id=")) {
					currElementName = "PMCluster";
					if (!writers.containsKey(currElementName)) {
						writers.put(currElementName, new RNCWriter(new File(dir, currElementName + ".xml")));
						writers.get(currElementName).println("<part>");
					}
					String right = line.substring(line.indexOf("<PMCluster id="));
					writers.get(currElementName).println(right);
				} else if (line.contains("<Operator id=")) {
					currElementName = "Operator";
					if (!writers.containsKey(currElementName)) {
						writers.put(currElementName, new RNCWriter(new File(dir, currElementName + ".xml")));
						writers.get(currElementName).println("<part>");
					}
					String right = line.substring(line.indexOf("<Operator id="));
					writers.get(currElementName).println(right);
				} else if (line.contains("</RNC>")) {
					if (writers.containsKey(currElementName)) {
						String left = line.substring(0, line.indexOf("</RNC>") + 6);
						rRemain = line.substring(line.indexOf("</RNC>") + 6);
						writers.get(currElementName).println(left);
						writers.get(currElementName).println("</part>");
						writers.get(currElementName).flush();
						writers.get(currElementName).close();
						currElementName = null;
					}
				} else if (line.contains("</Cluster>")) {
					if (writers.containsKey(currElementName)) {
						String left = line.substring(0, line.indexOf("</Cluster>") + 10);
						rRemain = line.substring(line.indexOf("</Cluster>") + 10);
						writers.get(currElementName).println(left);
						writers.get(currElementName).flush();
						currElementName = null;
					}
				} else if (line.contains("</BTSEquipment>")) {

					if (writers.containsKey(currElementName)) {
						String left = line.substring(0, line.indexOf("</BTSEquipment>") + 15);
						rRemain = line.substring(line.indexOf("</BTSEquipment>") + 15);
						writers.get(currElementName).println(left);
						writers.get(currElementName).flush();
						currElementName = null;
					}
				} else if (line.contains("</Site>")) {
					if (writers.containsKey(currElementName)) {
						String left = line.substring(0, line.indexOf("</Site>") + 7);
						rRemain = line.substring(line.indexOf("</Site>") + 7);
						writers.get(currElementName).println(left);
						writers.get(currElementName).flush();
						currElementName = null;
					}
				} else if (line.contains("</PMCluster>")) {
					if (writers.containsKey(currElementName)) {
						String left = line.substring(0, line.indexOf("</PMCluster>") + 12);
						rRemain = line.substring(line.indexOf("</PMCluster>") + 12);
						writers.get(currElementName).println(left);
						writers.get(currElementName).flush();
						currElementName = null;
					}
				} else if (line.contains("</Operator>")) {
					if (writers.containsKey(currElementName)) {
						String left = line.substring(0, line.indexOf("</Operator>") + 11);
						rRemain = line.substring(line.indexOf("</Operator>") + 11);
						writers.get(currElementName).println(left);
						writers.get(currElementName).flush();
						currElementName = null;
					}
				} else if (currElementName != null) {
					if (writers.containsKey(currElementName)) {
						writers.get(currElementName).println(line);
					}
				}
			}
			Iterator<String> it = writers.keySet().iterator();
			while (it.hasNext()) {
				String key = it.next();
				if (!key.startsWith("RNC_")) {
					writers.get(key).println("</part>");
				}
				IOUtils.closeQuietly(writers.get(key));
				results.add(writers.get(key).file);
			}
		} catch (Exception e) {
			logger.error("处理阿朗参数时发生异常：" + file.getAbsolutePath(), e);
			return null;
		} finally {
			IOUtils.closeQuietly(br);
			IOUtils.closeQuietly(reader);
			IOUtils.closeQuietly(in);
		}
		logger.debug("阿朗参数文件拆分完成 - " + results);
		file.delete();
		return results;
	}

	private static String findRNCName(String line) {
		int index = line.indexOf('\"');
		String tmp = line.substring(index + 1);
		index = tmp.indexOf('\"');
		tmp = tmp.substring(0, index);
		return tmp;
	}

	private static class RNCWriter extends PrintWriter {

		File file;

		public RNCWriter(File file) throws FileNotFoundException {
			super(file);
			this.file = file;
		}

	}

	private FileSplit() {
	}

	public static void main(String[] args) throws Exception {
		FileSplit.splitFull(new File("F:\\资料\\原始数据\\江苏阿朗参数\\UTRAN-SNAP20111212020002.xml"));
		// String line = "abc<RNC id=\"DQRNC02\"";
		// String right = line.substring(line.indexOf("<RNC id="));
		// String left = line.substring(0, line.indexOf("<RNC id="));
		// System.out.println(left);
		// System.out.println(right);
	}
}
