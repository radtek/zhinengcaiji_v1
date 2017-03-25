package parser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.poi.util.IOUtils;

import util.LogMgr;

import cn.uway.alarmbox.db.pool.DBUtil;
import framework.SystemConfig;

/**
 * 用于c+w广州测试的，根据文件名table_yyyymmdd.txt格式的，找出表名，然后直接入库 SimpleParser
 * 
 * @author 2012-9-24
 */
public class SimpleParser extends Parser {

	private static Set<String> tableNames = new HashSet<String>();

	private static final String FILE_DATE = "20120830";

	private static final String CONFIG = "." + File.separator + "conf" + File.separator + "gd_pk_tables.dat";

	static {
		// 初始化表 fileName直接配置基础数据SQL路径即可

		BufferedReader bufferReader = null;
		try {
			bufferReader = new BufferedReader(new FileReader(CONFIG));
			String line = null;

			while ((line = bufferReader.readLine()) != null) {
				tableNames.add(line.trim().toUpperCase());
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			// e.printStackTrace();
			Logger log = LogMgr.getInstance().getSystemLogger();
			log.error("加载 " + new File(CONFIG).getAbsolutePath() + " 异常", e);
		} finally {
			if (bufferReader != null) {
				IOUtils.closeQuietly(bufferReader);
			}

		}

	}

	public boolean parseData() throws Exception {
		File file = new File(fileName);
		String fileName = file.getName();
		int index = fileName.lastIndexOf("_");
		if (index == -1) {
			log.warn(this.collectObjInfo.getTaskID() + " " + fileName + " 格式不符合");
			return false;
		}

		String tableName = fileName.substring(0, index);
		// String tempName =
		// file.getName().substring(0,file.getName().indexOf(FILE_DATE)-1);
		if (!needParse(tableName.toUpperCase())) {

			log.warn(this.collectObjInfo.getTaskID() + " " + tableName + " 忽略");
			return false;
		}
		return parseAndWriteDatabase(file.getAbsolutePath(), tableName);
	}

	public boolean parseAndWriteDatabase(String fileName, String tableName) throws Exception {
		FileReader fileReader = null;
		BufferedReader bufferReader = null;

		// TODO 增加DB链接
		Connection con = getConnection();
		PreparedStatement tableStatement = null;
		if (con == null) {
			log.warn(this.collectObjInfo.getTaskID() + "-connection is null");
			return false;
		}

		// TODO 打开数据库链接
		try {
			String sql = "select * from " + tableName;
			tableStatement = con.prepareStatement(sql);
			ResultSetMetaData meta = tableStatement.getMetaData();

			fileReader = new FileReader(fileName);
			bufferReader = new BufferedReader(fileReader);
			String line = null;

			PreparedStatement statement = null;
			int num = 0;
			try {
				while ((line = bufferReader.readLine()) != null) {
					String[] infos = line.split("@@");
					if (infos.length - 2 != meta.getColumnCount())
						continue;
					if (num == 0) {
						String sqlstatement = createSQL(infos, meta, tableName);
						statement = con.prepareStatement(sqlstatement);
					}

					for (int i = 2; i < infos.length; i++) {
						String typeName = meta.getColumnClassName(i - 1);
						// 认为只有两种类型 string 和 数字类型

						if ("java.lang.String".equals(typeName)) {
							statement.setString(i - 1, infos[i]);
						} else {
							if (infos[i].equals("")) {
								infos[i] = "0";
							}
							statement.setBigDecimal(i - 1, new BigDecimal(infos[i]));
						}
					}
					//
					statement.addBatch();
					if (++num % 5000 == 0) {
						statement.executeBatch();
						statement.clearBatch();
					}

				}

				//
				if (num % 5000 != 0) {
					statement.executeBatch();
				}
				// 如果不是自动提交，就要自己提交
				if (!con.getAutoCommit()) {
					con.commit();
				}
			} catch (Exception e) {
				// TODO: 写日志
				log.warn(this.collectObjInfo.getTaskID() + "- 文件名" + fileName, e);
			} finally {
				// 关闭statement
				DBUtil.close(null, statement, null);

				// 关闭文件 流
				if (bufferReader != null) {
					IOUtils.closeQuietly(bufferReader);
				}
			}
		} catch (Exception e) {
			// TODO: 写日志
			log.warn(this.collectObjInfo.getTaskID() + "- 文件名" + fileName, e);
		} finally {
			// 关闭tableStatement con
			DBUtil.close(null, tableStatement, con);
		}

		return false;
	}

	static Connection getConnection() {
		SystemConfig systemConfig = SystemConfig.getInstance();

		String driver = systemConfig.getStoreDbDriver();
		String url = systemConfig.getStoreDbUrl();
		String username = systemConfig.getStoreDbUserName();
		String password = systemConfig.getStoreDbPassword();
		Connection connection = null;
		try {
			connection = DBUtil.getConnection(driver, url, username, password);
		} catch (Exception e) {
			// TODO写log
			Logger log = LogMgr.getInstance().getSystemLogger();
			log.error("数据库连接找开异常", e);

		}

		return connection;
	}

	public String createSQL(String[] infos, ResultSetMetaData meta, String tableName) throws SQLException, ClassNotFoundException {
		StringBuffer head = new StringBuffer("insert into " + tableName + "(");
		StringBuffer body = new StringBuffer("values(");
		for (int i = 2; i < infos.length; i++) {
			String columnName = meta.getColumnName(i - 1);
			if (i == infos.length - 1) {
				head.append(columnName + ")");
				body.append("?" + ")");
			} else {
				head.append(columnName + ",");
				body.append("?,");
			}
		}
		return head.append(body.toString()).toString();
	}

	public boolean needParse(String tableName) {
		return tableNames.contains(tableName);
	}

	private static Map<String, Object> getFileNames(String rootPath) {
		File thisDir = new File(rootPath);
		if (!thisDir.exists()) {
			throw new NullPointerException("File not exists");
		}
		File[] subList = thisDir.listFiles();
		Map<String, Object> returnMap = new HashMap<String, Object>();
		for (int i = 0; i < subList.length; i++) {
			String tableName = subList[i].getName().substring(0, subList[i].getName().indexOf(FILE_DATE) - 1);
			returnMap.put(tableName, null);
		}
		return returnMap;
	}

	private static ResultSetMetaData getTableMetaData(String tableName) throws SQLException {
		// TODO 打开数据库链接
		Connection connection = null;
		String sql = "select * from " + tableName;
		PreparedStatement statement = connection.prepareStatement(sql);
		ResultSetMetaData meta = statement.getMetaData();
		// TODO
		// DatabaseUtils.close(connection,statement,null);
		return meta;
	}

}
