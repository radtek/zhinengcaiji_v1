package parser.eric.cm;

import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Timestamp;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.log4j.Logger;

import util.LogMgr;
import util.Util;

/**
 * g网爱立信参数解析
 * 
 * @author ChenSijiang 2012-03-31
 */
public class GsmEricssonCmParser implements EricssonCmParser {

	private static final Logger log = LogMgr.getInstance().getSystemLogger();

	private static final String TYPE_FLAG = "<";

	private static final String END_FLAG = "END";

	@Override
	public void parse(String file, int omcId, Timestamp stampTime, long taskID) throws Exception {
		LineIterator lineIterator = null;
		InputStream in = null;
		try {
			in = new FileInputStream(file);
			lineIterator = IOUtils.lineIterator(in, null);
			String line = null;
			boolean isEmptyLine = false; // 用于标记上一行是否是空行
			boolean isFlagLine = false;// 用于标记上一行是否是类型标识行，即“<RLLBP;”这样的东西
			boolean isHeadLine = false;// 用于标记上一行是否是列头。
			boolean isDataLine = false;// 用于标记上一行是否是数据行。
			String currType = null;// 当前数据类型。
			while (lineIterator.hasNext()) {
				line = lineIterator.nextLine();
				if (Util.isNull(line)) {
					if (!isHeadLine) {
						// 当前是空行。
						isEmptyLine = true;
						isFlagLine = false;
					} else {
						// 虽然当前是空行，但上一行是列头，说明当前也是数据行，只是内容为空。
						log.debug("数据:" + line);
						isDataLine = true;
						isHeadLine = false;
					}
				} else if (line.startsWith(TYPE_FLAG)) {
					// 当前是标识行。
					isFlagLine = true;
					isEmptyLine = false;
					currType = line;
					log.debug("标识:" + currType);
				} else if (line.equalsIgnoreCase(END_FLAG)) {
					continue;
				} else {
					if (!isFlagLine && !isEmptyLine) {
						// 上一行不是空行，也不是标识行，说明是数据内容。
						log.debug("数据:" + line);
						isDataLine = true;
						isHeadLine = false;
					} else if (isEmptyLine) {
						// 上一行是空行，说明是列头。
						log.debug("列头:" + line);
						isHeadLine = true;
						isEmptyLine = false;
						isFlagLine = false;
						isDataLine = false;
					} else {
						// 到这里，说明上一行是标识行，而当前是非空行。
						// 那么，说明当前行是“BSC LOCATING DATA”这样的描述内容，跳过。
						isEmptyLine = false;
						isFlagLine = false;
						continue;
					}
				}
			}
		} catch (Exception e) {
			throw e;
		} finally {
			IOUtils.closeQuietly(in);
			if (lineIterator != null)
				lineIterator.close();
		}
	}

	public static void main(String[] args) {
		GsmEricssonCmParser parser = new GsmEricssonCmParser();
		try {
			parser.parse("C:\\Users\\ChenSijiang\\Desktop\\LYB65", 520, new Timestamp(Util.getDate1("2010-10-20 00:00:00").getTime()), 989);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
