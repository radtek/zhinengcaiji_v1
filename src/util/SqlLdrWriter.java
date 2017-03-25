package util;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.List;

/**
 * 用于写sqlldr文件。
 * 
 * @author ChenSijiang
 */
public class SqlLdrWriter {

	// 文件名（包括路径）
	private String fileName;

	// 表示累积多少条数据后写一入次，默认100
	private int backlogCount = 100;

	private int count = 0; // 记数，记录当前写到了第多少条数据

	private String charset; // 字符集

	private String tableName; // 表名

	// 列类型
	private List<ColumnType> columns;

	private FileWriter fileWriter;

	public SqlLdrWriter(String fileName) {
		super();
		this.fileName = fileName;
	}

	public SqlLdrWriter(String fileName, int backlogCount) {
		super();
		this.fileName = fileName;
		this.backlogCount = backlogCount;
	}

	/**
	 * 写数据
	 * 
	 * @param data
	 *            要写入的数据
	 * @param immediat
	 *            是否立即写入
	 * @throws Exception
	 */
	public void write(String data, boolean immediat) throws Exception {
		if (fileWriter == null) {
			open();
		}

		fileWriter.write(data + "\r\n");
		if (immediat) {
			fileWriter.flush();
		} else {
			if (count % backlogCount == 0) {
				fileWriter.flush();
			}
		}
		count++;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public int getBacklogCount() {
		return backlogCount;
	}

	public void setBacklogCount(int backlogCount) {
		this.backlogCount = backlogCount;
	}

	public String getCharset() {
		return charset;
	}

	public void setCharset(String charset) {
		this.charset = charset;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public List<ColumnType> getColumns() {
		return columns;
	}

	public void setColumns(List<ColumnType> columns) {
		this.columns = columns;
	}

	public void dispose() {
		try {
			fileWriter.flush();
			fileWriter.close();
			fileWriter = null;
			fileName = null;
			count = 0;
		} catch (Exception e) {
		}
	}

	/**
	 * 提交写文件操作，并创建控制文件
	 * 
	 * @throws Exception
	 */
	public void commit() throws Exception {
		try {
			fileWriter.flush();
		} catch (Exception e) {
		}

		// 创建控制文件
		createCltFile();
	}

	public void writeHead(String sign) throws Exception {
		int len = columns.size();
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < len - 1; i++) {
			ColumnType pe = columns.get(i);
			sb.append(pe.getColumnName()).append(sign);
		}
		ColumnType pe = columns.get(len - 1);
		sb.append(pe.getColumnName());

		write(sb.toString(), true);
	}

	private void open() throws Exception {
		fileWriter = new FileWriter(fileName);
	}

	/**
	 * 创建控制文件
	 * 
	 * @throws Exception
	 */
	private void createCltFile() throws Exception {
		String name = fileName.substring(0, fileName.lastIndexOf(".") + 1) + "clt";
		BufferedWriter bw = new BufferedWriter(new FileWriter(name, false));

		if (Util.isOracle()) {
			// bw.write("unrecoverable\r\n");
			bw.write("load data\r\n");

			if (Util.isNotNull(charset))
				bw.write("CHARACTERSET " + charset + " \r\n");
			else
				bw.write("CHARACTERSET AL32UTF8 \r\n");

			bw.write("infile '" + fileName + "'\r\n");
			bw.write("append into table " + tableName + " \r\n");
			bw.write("FIELDS TERMINATED BY \";\"\r\n");
			bw.write("TRAILING NULLCOLS\r\n");
			bw.write("(");

			StringBuilder sb = new StringBuilder();
			for (ColumnType ct : columns) {
				String columnName = ct.getColumnName();
				String type = ct.getType();
				if (type.equalsIgnoreCase("date")) {
					sb.append(columnName + " \"to_date(:" + columnName + ",'" + ct.getFormat() + "')\"").append(",");
				} else if (type.equalsIgnoreCase("lob")) {
					sb.append(columnName + " " + ct.getFormat() + ",");
				} else {
					sb.append(columnName + ",");
				}
			}

			String str = sb.toString();
			bw.write(str.substring(0, str.length() - 1));

			bw.write(")");
			bw.write("\r\n");

		} else if (Util.isSybase() || Util.isSqlServer()) {
			bw.write("10.0\r\n"); // BCP版本
			int size = columns.size();
			bw.write(String.valueOf(size) + "\r\n"); // 字段个数

			int i = 1;
			for (ColumnType ct : columns) {
				String columnName = ct.getColumnName();

				bw.write(i + "\tSYBCHAR\t0\t128\t");
				if (i < size) {
					bw.write("\";\"");
				} else {
					bw.write("\"\n\"");
				}

				bw.write("\t" + i + "\t" + columnName + "\r\n");

				i++;
			}
		}

		bw.close();
	}
}
