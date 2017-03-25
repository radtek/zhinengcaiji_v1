package parser.eric.pm;

import java.io.Closeable;
import java.io.File;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.commons.io.IOUtils;

import task.CollectObjInfo;
import task.RegatherObjInfo;
import util.Util;
import framework.SystemConfig;

/**
 * 并发SQLLDR入库工具。用于W网爱立信性能入库，以一个时间点的任务为单位入库。即一个时间点的任务，把所有文件都解析写入TXT文件后，入库， 而不是每个文件写一次SQLLDR文件入库。
 * 
 * @author ChenSijiang
 * 
 */
public final class ConcurrentSqlldrUtilXXX {

	// 存放每个任务，每个时间点的SQLLDR信息。 Map的key为"taskid - key - time"的格式，例如:
	// "221013700 - 221013700 - 2011-01-14 14:00:00"。
	private static final Map<String, Map<String/* 这个String是表名 */, SqlldrInfo>> MAP = new HashMap<String, Map<String, SqlldrInfo>>();

	private static final Random RND = new Random();

	/**
	 * 放入SQLLDR数据并写入文件。
	 * 
	 * @param task
	 *            所属任务信息。
	 * @param sqlldrParam
	 *            SQLLDR信息。
	 */
	public static void putSqlldrParam(CollectObjInfo task, SqlldrParam sqlldrParam) {
		long id = task.getTaskID();
		if (task instanceof RegatherObjInfo)
			id = task.getKeyID() - 10000000;
		String key = String.format("%s - %key - %s", task.getTaskID(), id, Util.getDateString(task.getLastCollectTime()));
		SqlldrInfo info = null;
		synchronized (MAP) {
			if (MAP.containsKey(key)) {
			} else {

			}
		}
	}

//	private static SqlldrInfo readyForSqlldr(CollectObjInfo task, SqlldrParam sqlldrParam) {
//		File dir = new File(SystemConfig.getInstance().getCurrentPath() + File.separator + "ldrlog" + File.separator + "eric_w_pm" + File.separator
//				+ task.getTaskID() + File.separator);
//		dir.mkdirs();
//		String strDateTime = Util.getDateString_yyyyMMddHHmmss(task.getLastCollectTime());
//		String tn = sqlldrParam.tbName.toUpperCase();
//		int rnum = RND.nextInt(Integer.MAX_VALUE);
//		String name = task.getTaskID() + "_" + tn + "_" + strDateTime + "_" + rnum;
//		SqlldrInfo info = new SqlldrInfo(tn, new File(dir, name + ".txt"), new File(dir, name + ".log"), new File(dir, name + ".bad"), new File(dir,
//				name + ".ctl"));
//		return info;
//	}

	private static class SqlldrInfo implements Closeable {

		String tableName;

		File txt;

		File log;

		File bad;

		File ctl;

		PrintWriter writerForTxt;

		PrintWriter writerForCtl;

		public SqlldrInfo(String tableName, File txt, File log, File bad, File ctl) {
			super();
			this.tableName = tableName;
			this.txt = txt;
			this.log = log;
			this.bad = bad;
			this.ctl = ctl;
			try {
				writerForTxt = new PrintWriter(txt);
				writerForCtl = new PrintWriter(ctl);
			} catch (Exception unused) {
			}
		}

		@Override
		public void close() {
			IOUtils.closeQuietly(writerForCtl);
			IOUtils.closeQuietly(writerForTxt);

		}

		@Override
		public int hashCode() {
			return tableName.hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			SqlldrInfo info = (SqlldrInfo) obj;
			return info.tableName.equalsIgnoreCase(this.tableName);
		}

	}

	private ConcurrentSqlldrUtilXXX() {
	}
}
