package tools.db;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Formatter;
import java.util.List;

import util.CommonDB;
import util.Util;
import framework.SystemConfig;

/**
 * 表结构比较工具
 * 
 * @author litp Jul 27, 2010
 * @since 1.0
 */
public class TableStructureCompare {

	private TxtFileFormatter writer = null;

	private CSVFileReader reader = null;

	public TableStructureCompare() {
		super();
	}

	public void destory() {
		if (reader != null)
			reader.close();

		if (writer != null)
			writer.close();
	}

	public void build(String fileName) throws Exception {
		reader = new CSVFileReader(fileName);
		writer = new TxtFileFormatter();
		SystemConfig config = SystemConfig.getInstance();
		String destDriver = config.getDbDriver();
		String destUrl = config.getDbUrl();
		String destName = config.getDbUserName();
		String destPwd = config.getDbPassword();
		String[] line = null;
		while ((line = reader.getALine()) != null) {
			if (line.length != 6) {
				System.out.println("格式不正确,跳过. " + Arrays.toString(line));
				continue;
			}

			String srcTab = line[0]; // 厂家原始表名
			String destTab = line[1]; // 采集表名
			String srcDriver = line[2]; // 驱动
			String srcUrl = line[3]; // URL
			String srcName = line[4]; // 用户名
			String srcPwd = line[5]; // 密码

			if (Util.isNull(srcTab) && Util.isNull(destTab))
				continue;

			if (Util.isNotNull(srcTab) && (Util.isNull(srcDriver) || Util.isNull(srcUrl) || Util.isNull(srcName) || Util.isNull(srcPwd))) {
				System.out.println("驱动/URL/用户名/密码 有空值,跳过. " + Arrays.toString(line));
				continue;
			}

			String[] srcColNames = getColumns(srcTab, srcDriver, srcUrl, srcName, srcPwd);
			String[] destColNames = getColumns(destTab, destDriver, destUrl, destName, destPwd);

			write(srcColNames, srcTab, destColNames, destTab);
		}

		String oFileName = writer.getFileName();
		System.out.println("文件 " + oFileName + " 已生成");

		reader.close();
		writer.close();
	}

	private void write(String[] srcColNames, String srcTab, String[] destColNames, String destTab) {
		int currMaxLenA = getMaxLength(srcColNames); // 最大长度
		if (srcTab.length() > currMaxLenA)// 和表名一起比较
			currMaxLenA = srcTab.length();

		writer.setFormatStrLen(currMaxLenA);

		String lineA = srcTab + "\n" + Arrays.toString(srcColNames);
		String lineB = destTab + "\n" + Arrays.toString(destColNames);

		srcTab = srcTab == null ? "" : srcTab;
		destTab = destTab == null ? "" : destTab;
		writer.writeln(srcTab, destTab);
		writer.writeln("---------------------------------------------", "------------------------------");

		writeColumns(srcColNames, destColNames);

		writer.writeln("---------------------------------------------", "------------------------------");
		writer.write(lineA);
		writer.write(lineB);
		writer.write2n("----------------------------------------------------------------------\n\n");
		writer.flush();
	}

	/**
	 * 获取数组中字符串的最大长度
	 */
	private int getMaxLength(String[] names) {
		int max = 0;
		for (String str : names) {
			int len = str.length();
			if (len > max)
				max = len;
		}

		return max;
	}

	private void writeColumns(String[] arrA, String[] arrB) {
		int aLen = 0;
		int bLen = 0;

		if (arrA != null)
			aLen = arrA.length;

		if (arrB != null)
			bLen = arrB.length;

		if (aLen == 0 && bLen == 0)
			return;

		int min = aLen >= bLen ? bLen : aLen;
		for (int i = 0; i < min; i++) {
			writer.writeln(arrA[i], arrB[i]);
		}

		if (aLen >= bLen) {
			for (int i = min; i < aLen; i++) {
				writer.writeln(arrA[i], "");
			}
		} else {
			for (int i = min; i < bLen; i++) {
				writer.writeln("", arrB[i]);
			}
		}

	}

	private String[] getColumns(ResultSetMetaData meta) {
		if (meta == null)
			return null;

		String[] names = null;
		try {
			int count = meta.getColumnCount();
			names = new String[count];
			for (int i = 1; i <= count; i++) {
				String colName = meta.getColumnName(i);
				names[i - 1] = colName;
			}
			Arrays.sort(names);
		} catch (Exception e) {
			System.out.println("获取字段名异常,原因: " + e.getMessage());
		}

		return names;
	}

	/**
	 * 获取元数据
	 */
	private String[] getColumns(String tbName, String drivClass, String url, String name, String pwd) {
		if (Util.isNull(tbName))
			return null;

		ResultSetMetaData meta = null;
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		conn = CommonDB.getConnection(0, drivClass, url, name, pwd);

		String[] result = null;

		String sql = "select * from " + tbName + " where 1=0";
		try {
			if (conn != null) {
				ps = conn.prepareStatement(sql);
				rs = ps.executeQuery();
				meta = rs.getMetaData();
				result = getColumns(meta);
			}
		} catch (SQLException e) {
			System.out.println("执行查询异常:" + e.getMessage());
		} finally {
			CommonDB.close(rs, ps, conn);
		}

		return result;
	}

	private class TxtFileFormatter {

		private static final String COMPARE_FILENAME_PREFIX = "tableCompare";

		private String formatStr = null;

		private Formatter f = null;

		private String fileName;

		public TxtFileFormatter() throws Exception {
			init();
		}

		public String getFileName() {
			return fileName;
		}

		private void init() throws Exception {
			String fName = COMPARE_FILENAME_PREFIX + "_" + Util.getDateString_yyyyMMddHHmmss(new Date()) + ".txt";
			String fPath = System.getProperty("user.dir") + File.separator + fName;
			fileName = fPath;
			f = new Formatter(new File(fPath));
		}

		public void write(String param) {
			if (f != null && param != null) {
				f.format("%s\n", param);
			}
		}

		public void writeln(String param1, String param2) {
			if (f != null && param1 != null && param2 != null) {
				f.format(formatStr + "\n", param1, param2);
			}
		}

		public void write2n(String param) {
			if (f != null && param != null) {
				f.format("%s\n\n", param);
			}
		}

		public void flush() {
			if (f != null) {
				f.flush();
			}
		}

		public void close() {
			if (f != null) {
				f.flush();
				f.close();
				f = null;
			}
		}

		public void setFormatStrLen(int maxLen) {
			String formatStr = "%1$-" + maxLen + "." + maxLen + "s\t\t%2$s";
			this.formatStr = formatStr;
		}
	}

	/**
	 * CSV文件Reader
	 * 
	 * @author litp Jul 27, 2010
	 * @since 1.0
	 */
	private class CSVFileReader {

		private String fileName;

		private List<String[]> list = null;

		private BufferedReader reader;

		private static final String CSV_DELIMIT = ",";

		public CSVFileReader(String fileName) throws Exception {
			this.fileName = fileName;
			readFile();
		}

		private void readFile() throws Exception {
			File f = new File(fileName);
			reader = new BufferedReader(new FileReader(f));
			list = new ArrayList<String[]>();
			String line = null;
			// 第一行表头不用读取
			reader.readLine();
			while ((line = reader.readLine()) != null) {
				list.add(line.split(CSV_DELIMIT));
			}
		}

		// 获取一行数据
		public String[] getALine() {
			String[] line = null;
			if (list != null && !list.isEmpty()) {
				line = list.remove(0);
			}
			return line;
		}

		public void close() {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
				}
			}
		}
	}

	public static void main(String[] args) {
		if (args.length != 1) {
			System.out.println("参数不正确");
			return;
		}
		String fileName = args[0];
		TableStructureCompare tc = new TableStructureCompare();
		try {
			tc.build(fileName);
		} catch (Exception e) {
			System.out.println("操作失败,原因: " + e.getMessage());
		} finally {
			tc.destory();
		}
		System.out.println("生成完成");
	}
}
