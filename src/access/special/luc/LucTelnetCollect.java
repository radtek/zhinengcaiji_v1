package access.special.luc;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import task.CollectObjInfo;
import util.LogMgr;
import util.Util;
import collect.FTPTool;
import framework.SystemConfig;

/**
 * 专门针对C网阿朗TELNET方式采集。
 * 
 * @author ChenSijiang
 */
public class LucTelnetCollect {

	/* 缓存阿朗.SCT模板 */
	private static final Map<String/* 小写模板名，如cdhfl.sct，不带目录 */, Map<Integer/* 字段位置 */, String/* 字段名 */>> SCT_CACHE = new HashMap<String, Map<Integer, String>>();

	private CollectObjInfo task;

	private String logKey;

	private FTPTool ftpTool;

	private static final Logger logger = LogMgr.getInstance().getSystemLogger();

	private static final SystemConfig cfg = SystemConfig.getInstance();

	public static Map<Integer, String> parseSCT(InputStream in) {
		Map<Integer, String> map = new HashMap<Integer, String>();
		Reader reader = null;
		BufferedReader br = null;
		try {
			reader = new InputStreamReader(in);
			br = new BufferedReader(reader);
			String line = null;
			StringBuilder buff = new StringBuilder();
			boolean findOut = false;
			int index = 0;
			while ((line = br.readLine()) != null) {
				line = line.trim().toLowerCase();
				line = line.replace("\t", " ");
				if (line.startsWith("output:")) {
					line = line.substring(line.indexOf(":") + 1);
					buff.append(line).append(" ");
					findOut = true;
				} else if (findOut) {
					if (!line.contains(":") && !line.contains("enddb")) {
						buff.append(line).append(" ");
					} else {
						findOut = false;
					}
				}
			}
			String[] sp = buff.toString().split(" ");
			for (String s : sp) {
				if (Util.isNull(s))
					continue;
				map.put(index++, s.trim());
			}
		} catch (Exception e) {
			logger.error("解析朗讯SCT模板时发生异常", e);
			return null;
		} finally {
			IOUtils.closeQuietly(br);
			IOUtils.closeQuietly(reader);
			IOUtils.closeQuietly(in);
		}

		return map;
	}

	public static Map<Integer, String> parseSCT(File sctFile) {
		try {
			return parseSCT(new FileInputStream(sctFile));
		} catch (Exception e) {
			logger.error("解析朗讯SCT模板时发生异常", e);
			return null;
		}
	}

	public static Map<Integer, String> parseSCT(String content) {
		InputStream in = new ByteArrayInputStream(content.getBytes());
		return parseSCT(in);
	}

	public static void main(String[] args) {
		Map<Integer, String> map = parseSCT(new File("C:\\Users\\ChenSijiang\\Desktop\\2012-12-12\\cdhfl.sct"));
		System.out.println(map.get(199));
	}
}
