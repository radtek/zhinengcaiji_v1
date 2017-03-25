package parser.hw.mml;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.jsp.jstl.sql.Result;

import org.apache.log4j.Logger;

import task.CollectObjInfo;
import util.CommonDB;
import util.DbPool;
import util.LogMgr;
import util.Util;

/**
 * HuaWei MML 参数数据文件自动采集
 * 
 * @author ltp Jan 8, 2010
 * @since 1.0
 */
public class HwMmlBProcessor {

	private static final Map<String, List<String>> TABLE_COLS = new HashMap<String, List<String>>();

	private String rncid = "";

	// 用于存放数据表的名字和此表的所有字段
	private Map<String, List<String>> tables = null;

	// 用于存放最后解析得到的数据<tableName,此表的所有数据<一条数据<列名和数据>>>
	private Map<String, List<List<Cell>>> datas = null;

	private String additional = null;

	private final String heads = "ADD,SET,MOD";

	/**
	 * U开头的表名
	 */
	private static final List<String> SPEC = new ArrayList<String>();

	// private static final Map<String, List<String>> COLS = new HashMap<String,
	// List<String>>();

	private CollectObjInfo collectObjInfo = null;

	private long taskId = 0;

	private final static Logger log = LogMgr.getInstance().getSystemLogger();
	static {
		SPEC.add("URA");
		SPEC.add("USERPLNSHAREPARA");
		SPEC.add("USERHAPPYBR");
		SPEC.add("UESTATETRANSTIMER");
		SPEC.add("UESTATETRANS");
		SPEC.add("USERMBR");
		SPEC.add("UIA");
		SPEC.add("UEA");
		SPEC.add("USERPRIORITY");
		SPEC.add("USERGBR");
		// SPEC.add("UCONNMODETIMER");
		// SPEC.add("UUESTATETRANS");
		// SPEC.add("UCELLHSDPA");
		// SPEC.add("UCELLSELRESEL");

		Connection con = null;
		PreparedStatement st = null;
		ResultSet rs = null;
		try {
			con = DbPool.getConn();
			String sql = "select table_name,column_name from user_tab_cols   where TABLE_NAME like ?";
			st = con.prepareStatement(sql);
			st.setString(1, "CLT_CM_W_HW_%");
			rs = st.executeQuery();
			while (rs.next()) {
				String tn = rs.getString("table_name");
				String cn = rs.getString("column_name");
				List<String> cols = null;
				if (TABLE_COLS.containsKey(tn))
					TABLE_COLS.get(tn).add(cn);
				else {
					cols = new ArrayList<String>();
					cols.add(cn);
					TABLE_COLS.put(tn, cols);
				}
			}
		} catch (Exception e) {
			log.error("载入华为W参数表结构时出错");
		} finally {
			CommonDB.closeDBConnection(con, st, rs);
		}
	}

	// 更改
	private final String hedReg = "^(\\w+)\\s+([^:]+):(.*?);$";

	private final String cntReg = "([^=]+)=([^,]+)(?:,\\s)?";

	private Matcher hedMhr = null;

	private Matcher cntMhr = null;

	public HwMmlBProcessor() {
		super();
		tables = new HashMap<String, List<String>>();
		datas = new HashMap<String, List<List<Cell>>>();
		additional = "CLT_CM_W_HW_";
		//
		hedMhr = Pattern.compile(hedReg).matcher("");
		cntMhr = Pattern.compile(cntReg).matcher("");
	}

	/**
	 * 将列名添加到Map中
	 * 
	 * @param tableName
	 *            表名
	 * @param columnName
	 *            对应的列名
	 */
	private void addTable(String tableName, String columnName) {
		if (!tables.containsKey(tableName)) {
			List<String> list = new ArrayList<String>();
			list.add(columnName);
			tables.put(tableName, list);
		} else {
			List<String> list = tables.get(tableName);
			if (!list.contains(columnName)) {
				list.add(columnName);
			}
		}
	}

	/**
	 * 将数据添加到Map中
	 * 
	 * @param tableName
	 *            表名
	 * @param list
	 *            此表的所有数据
	 */
	private void addData(String tableName, List<Cell> list) {
		if (!datas.containsKey(tableName)) {
			List<List<Cell>> tabList = new ArrayList<List<Cell>>();
			tabList.add(list);
			datas.put(tableName, tabList);
		} else {
			List<List<Cell>> tabList = datas.get(tableName);
			tabList.add(list);
		}
	}

	/**
	 * 按文件名读取文件(表名为原始字符串和additional相联后截取的长度不大于30个字符的字符串， 字段名为截取后的长度不大于30个字符的字符串)
	 * 
	 * @param fileName
	 *            文件名
	 * @throws Exception
	 */
	private void read(String fileName) throws Exception {
		File file = new File(fileName);
		if (!file.exists()) {
			log.error("文件不存在：" + fileName);
			return;
		}
		FileInputStream in = null;
		BufferedReader br = null;
		try {
			in = new FileInputStream(file);
			br = new BufferedReader(new InputStreamReader(in));
			readHead(br);// 读取头部
			readBody(br);// 读取数据
		} finally {
			if (in != null) {
				in.close();
			}
			if (br != null) {
				br.close();

			}
		}
	}

	/**
	 * 读取头部信息
	 * 
	 * @param br
	 * @throws IOException
	 */
	private void readHead(BufferedReader br) throws IOException {
		if (br == null) {
			return;
		}
		// 用于存放每次读取的一行时的 临时值
		String tempLine = null;
		int i = 1;
		while ((tempLine = br.readLine()) != null) {

			if (tempLine.equals(""))
				continue;
			// 忽略的行数
			if (i <= 7) {
				// 读出RNCID后面的值
				if (tempLine.contains("BSCID:") || tempLine.contains("RNCID:")) {
					setRncid(tempLine);
				}
				i++;
				continue;
			}
			break;
		}

	}

	/**
	 * 读取数据部分信息
	 * 
	 * @param br
	 * @throws IOException
	 */
	private void readBody(BufferedReader br) throws IOException {
		if (br == null) {
			return;
		}
		// 用于存放每次读取的一行时的 临时值
		String tempLine = null;
		while ((tempLine = br.readLine()) != null) {

			if (tempLine.equals(""))
				continue;
			// skip_line行以后就为真正解析的数据
			parseContent(tempLine);
		}// while
	}

	// 获取rncid
	private void setRncid(String dataLine) {
		int mark = dataLine.indexOf(":");
		// 当mark+1时，可以适合":"号后面有或没有空格以及多个空格的情况
		try {
			rncid = dataLine.substring(mark + 1).trim();
			if (!rncid.startsWith("'") && !rncid.endsWith("'")) {
				rncid = ("'" + rncid + "'");
			}
		} catch (Exception e) {
			log.error("Task-" + taskId + "：RNCID截取时错误！");
		}
	}

	/**
	 * 解析数据部份
	 * 
	 * @param line
	 *            一行的数据部分
	 */
	private void parseContent(String line) {
		hedMhr.reset(line);
		if (!hedMhr.find()) {
			return;
		}
		// 记录头
		String head = hedMhr.group(1);
		if (!filter(head)) {
			return;
		}
		// 表名
		String tableName = hedMhr.group(2);

		if (!SPEC.contains(tableName)) {
			if (tableName.startsWith("U")) {
				tableName = tableName.substring(1);
			}
		}
		// 内容，数据部份
		String content = hedMhr.group(3);
		if (tableName != null && !tableName.equals("") && content != null && !content.equals("")) {
			tableName = additional + tableName;
			if (tableName.length() > 30) {
				tableName = tableName.substring(0, 30);
			}
			//
			List<Cell> row = new ArrayList<Cell>();
			cntMhr.reset(content);
			while (cntMhr.find()) {
				String columnName = cntMhr.group(1);
				// columnName长度不能大于30个字符
				if (columnName.length() > 30) {
					columnName = columnName.substring(0, 30);
				}
				columnName = "\"" + columnName + "\"";
				String value = cntMhr.group(2);
				addTable(tableName, columnName);
				Cell record = new Cell();
				record.setColumnName(columnName);
				record.setValue(value);
				row.add(record);
			}
			addData(tableName, row);
		}

	}

	/**
	 * 过滤ADD SET MOD开头的数据行
	 * 
	 * @param head
	 *            表头
	 * @return
	 */
	private boolean filter(String head) {
		boolean flag = false;
		if (head != null && (heads.contains(head))) {
			flag = true;
		}
		return flag;
	}

	/**
	 * 获取需要额外添加的列
	 * 
	 * @param tableName
	 *            表名
	 * @param columns
	 *            此表名对应的所有列
	 * @return
	 */
	private List<String> needAddColumns(String tableName, List<String> columns) {
		List<String> addColumns = null;
		// 在tb_dict表中查询出指定表的所以列
		String sql = "select \"COLUMNNAME\" from CLT_CM_W_HW_MAP where tablename='" + tableName + "'";
		List<String> queryColumns = query(sql);
		// 如果allColumn不为空,那么此表已存在;否则不存在就直接添加表
		if (queryColumns != null && queryColumns.size() != 0) {
			// 存放原表中不存在的列
			addColumns = new ArrayList<String>(3);
			// 查找要新建表的列名在原表中是否存在
			for (String column : columns) {
				if (!queryColumns.contains(column)) {
					addColumns.add(column);
				}
			}
		}
		return addColumns;
	}

	/**
	 * 查询某个表的所有列
	 * 
	 * @param sql
	 *            查询语句
	 * @return list
	 */
	@SuppressWarnings("rawtypes")
	private List<String> query(String sql) {
		List<String> list = null;
		if (sql != null && !"".equals(sql)) {
			Result rs = null;
			try {
				rs = CommonDB.queryForResult(sql);
			} catch (Exception e) {
				log.error("Task-" + taskId + "：查询时发生错误：" + sql + "", e);
			}
			if (rs != null) {
				list = new ArrayList<String>();
				for (SortedMap row : rs.getRows()) {
					String column = "";

					try {
						column = row.get("COLUMNNAME").toString();
					} catch (Exception e) {
					}
					if (Util.isNull(column))
						column = row.get("columnname").toString();
					if (column != null && !column.equals(""))

					{
						if (column.contains("'")) {
							column = column.replace("'", "''");
						}
						list.add(column);
					}

				}
			}
		}

		return list;
	}

	/**
	 * 将表插入到数据库中
	 */
	private void createTable() {
		StringBuilder sql = null;
		Set<String> tabs = tables.keySet();
		for (String table : tabs) {
			if (table == null || table.length() < 1)
				continue;
			// 得到此表的所有列
			List<String> columns = tables.get(table);
			if (columns != null && columns.size() != 0) {
				List<String> addColumns = needAddColumns(table, columns);
				// 如果addColumns中有列名,那么就将这些列添加到原表中!
				if (addColumns != null && addColumns.size() > 0) {
					if (addColumns.size() != 0) {
						String addSql = "alter table " + table + " add(";
						for (String string : addColumns) {
							addSql = addSql + string + " varchar2(1055),";
							//
							addColumn(table, string);
						}
						addSql = addSql.substring(0, addSql.length() - 1) + ")";
						// 向原表添加新的列
						// try
						// {
						// CommonDB.executeUpdate(addSql);
						// }
						// catch (SQLException e)
						// {
						// if ( e.getErrorCode() != 955 )
						// log.error("Task-" + taskId + "：创建表" + table
						// + "异常！", e);
						// }
						// log.info("Task-" + taskId + "：新增列名语句： " + addSql);
					}
				}
				// 此表不存在
				else {
					// sql = new StringBuilder("create table ");
					// sql.append(table);
					// sql.append("(\"OMCID\" NUMBER,\"COLLECTTIME\" DATE,\"STAMPTIME\" DATE,\"RNCID_X\" NUMBER,");
					// int size = columns.size() - 1;
					// for (int i = 0; i < size; i++)
					// {
					// sql.append(columns.get(i)).append(" varchar2(1055),");
					// }
					// sql.append(columns.get(size)).append(" varchar2(1055))");
					// int i = -1;
					// try
					// {
					// i = CommonDB.executeUpdate(sql.toString());
					// }
					// catch (SQLException e)
					// {
					// if ( e.getErrorCode() != 955 )
					// log.error("Task-" + taskId + "：建表错误：", e);
					// }
					// // log.info("Task-" + taskId + "：建表语句： " +
					// sql.toString());
					// // 如果建表成功(因为要先建表成功后才向表中添加)
					// if ( i != -1 )
					// {
					// for (String column : columns)
					// {
					// addColumn(table, column);
					// }
					// }
				}
			}// if
		}// for
	}

	// 向tb_dict表中添加数据(表名和列名)
	private int addColumn(String tableName, String column) {
		int result = -1;
		String addColumn = "insert into CLT_CM_W_HW_MAP(tablename,\"COLUMNNAME\") values('" + tableName + "','" + column + "')";
		try {
			result = CommonDB.executeUpdate(addColumn);
		} catch (SQLException e) {
			result = -1;
			log.error("Task-" + taskId + "：插入列是发生错误：" + addColumn + ":", e);
		}
		return result;
	}

	/**
	 * 插入数据(将所有解析得到的数据插入到数据库中)
	 * 
	 * @param omcid
	 *            IGP_CONF_DEVICE表的omcid
	 */
	private void insertData() {
		if (datas == null || datas.isEmpty()) {
			return;
		}
		// omcid
		int omcid = collectObjInfo.getDevInfo().getOmcID();
		String stamptime = Util.getDateString(collectObjInfo.getLastCollectTime());

		List<String> sqlList = null;
		Set<String> tables = datas.keySet();
		for (String table : tables) {
			if (!TABLE_COLS.containsKey(table))
				continue;
			sqlList = new ArrayList<String>();
			int[] count = null;
			if (table == null || table.length() < 1)
				continue;
			List<List<Cell>> tableDa = datas.get(table);
			for (List<Cell> list : tableDa) {
				// 最终在数据库中存在的表名dbTable
				String sql = "insert into " + table + "(\"OMCID\",\"COLLECTTIME\",\"STAMPTIME\",\"RNCID_X\",";
				String columnNames = "";
				String columnValues = " values(" + omcid + ",sysdate" + ",to_date('" + stamptime + "','YYYY-MM-DD HH24:MI:SS')" + "," + rncid + ",'";
				for (Cell record : list) {
					if (TABLE_COLS.get(table).contains(record.getColumnName().replace("\"", ""))) {
						columnNames += record.getColumnName() + ",";
						columnValues += record.getValue() + "','";
					}
				}
				sql += columnNames.substring(0, columnNames.length() - 1) + ")" + columnValues.substring(0, columnValues.length() - 2) + ")";
				sqlList.add(sql);
			}// for

			try {
				count = CommonDB.executeBatch(sqlList);
			} catch (SQLException e) {
				if (e.getErrorCode() != 17081) {
					log.error("Task-" + taskId + "：华为参数提交错误！", e);
				}
			}
			if (count != null) {
				LogMgr.getInstance().getDBLogger().log(omcid, table, stamptime, count.length, taskId);
				log.debug("Task-" + taskId + "：插入记录数据库日志:表名" + table + ":条数" + count.length);
			}
		}// for
	}

	/**
	 * 创建表字典 tb_dict
	 */
	private void createTB() {
		String sqlTB = "create table CLT_CM_W_HW_MAP(tablename varchar2(55),\"COLUMNNAME\" varchar2(55))";
		try {
			CommonDB.executeUpdate(sqlTB);
		} catch (SQLException e) {
			if (e.getErrorCode() != 955)
				log.error("Task-" + taskId + "：创建表CLT_CM_W_HW_MAP异常！", e);
		}
	}

	public void process(String fileName, CollectObjInfo collectObjInfo) throws Exception {
		this.collectObjInfo = collectObjInfo;
		this.taskId = collectObjInfo.getTaskID();
		createTB();
		read(fileName);
		createTable();
		insertData();
	}

	/**
	 * 与此业务相关的类
	 * 
	 * @author ltp Apr 23, 2010
	 * @since 1.0
	 */
	class Cell {

		// 列名
		private String columnName;

		// 值
		private String value;

		public String getColumnName() {
			return columnName;
		}

		public void setColumnName(String columnName) {
			this.columnName = columnName;
		}

		public String getValue() {
			return value;
		}

		public void setValue(String value) {
			if (value != null && !value.equals("")) {
				// 如果以(")开头,就删除(""),
				if (value.startsWith("\"")) {
					value = value.substring(1, value.length() - 1);
				}
				// 如果含有"'"单引号,那么就改为"''"
				if (value.contains("'")) {
					value = value.replace("'", "''");
				}
			}
			this.value = value;
		}
	}
}
