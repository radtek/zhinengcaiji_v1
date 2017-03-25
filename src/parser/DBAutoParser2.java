package parser;

import java.io.File;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import sqlldr.SqlldrImpl;
import store.AbstractStore;
import store.StoreFactory;
import task.CollectObjInfo;
import task.DevInfo;
import templet.DBAutoTempletP;
import templet.DBAutoTempletP2;
import templet.Table;
import templet.Table.Column;
import util.CommonDB;
import util.Util;
import access.DBAutoAccessor;
import distributor.DistributeTemplet;
import exception.StoreException;
import framework.ConstDef;
import framework.SystemConfig;

/**
 * 数据库接入方式，无需配置模块(解析、分发模板在一起)
 * 
 * @author liuwx 2010-7-21
 * @since 1.0
 * @see DBAutoAccessor
 * @see DBAutoTempletP2
 */
public class DBAutoParser2 extends Parser {

	/** Map<表名，<采集表字段名,厂家字段名>> */
	private Map<String, Map<String, String>> mappingfields = null;

	protected List<File> clobFiles = new ArrayList<File>();

	protected int clobIndex;

	protected static String DELIMIT = "!"; // 分隔符

	// 江苏LTE4G网络采集，采集表中用mmeid替换了omcid：解析时把omcid作为mmeid入库
	protected String is4G = SystemConfig.getInstance().getLteIs4G();

	public DBAutoParser2() {

	}

	public int parseData(ResultSet rs, DBAutoTempletP2.Templet temP) throws Exception {
		// 统一分隔符
		DELIMIT = SqlldrImpl.splitStr;

		int recordCount = 0;

		AbstractStore<?> sqlldrStore = null;

		// 获取此模板所有的字段映射
		DBAutoTempletP2 temp = (DBAutoTempletP2) collectObjInfo.getParseTemplet();
		mappingfields = temp.getMappingfields();

		defaultValueColumns = temP.getDefaultValueColumns();

		// 设置特殊字段过滤html标签内容
		setHtmlTagsFilterColumnsSet(temP);

		// 获取采集表结构
		String desTable = temP.getDestTableName();

		String locsql = toSql(desTable, null);
		locsql += " where 1=2";
		Connection destConn = null;
		PreparedStatement descPs = null;
		ResultSet destRs = null;
		try {
			destConn = CommonDB.getConnection();
			descPs = destConn.prepareStatement(locsql);
			destRs = descPs.executeQuery();

			// 厂家是否是oracle数据库
			boolean isOracle = collectObjInfo.getDBDriver().contains("oracle");

			ResultSetMetaData srcMeta = rs.getMetaData(); // 厂家表结构
			ResultSetMetaData destMeta = destRs.getMetaData(); // 采集表结构

			Table table = new Table();
			table.setName(temP.getDestTableName());
			table.setId(temP.getId());
			table.setSplitSign(DELIMIT);
			// 解析元数据
			parseMeta(srcMeta, destMeta, table);

			Collection<Column> colection = table.getColumns().values();
			// 处理数据
			while (rs.next()) {
				StringBuilder colVals = new StringBuilder();
				for (Column col : colection) {
					int extIndex = col.getExtIndex();

					// 厂家表没有该字段,默认索引为0
					if (extIndex == 0) {
						colVals.append(defaultValueValidate(col, "")).append(DELIMIT);
						continue;
					}

					String colVal = null;
					if (col.getType() != 4)// 采集表字段类型，大字段类型2005映射為4
					{
						colVal = rs.getString(extIndex);// 列值

						colVal = defaultValueValidate(col, colVal);

						colVal = htmlTagsFilter(col, colVal);

						colVals.append(removeNoise(col.getType(), colVal)).append(DELIMIT);
					} else {
						if (isOracle) {
							colVal = ConstDef.ClobParse(rs.getClob(extIndex));
						} else
							colVal = rs.getString(extIndex);// 列值

						colVal = defaultValueValidate(col, colVal);

						colVal = htmlTagsFilter(col, colVal);

						File clob = new File(SystemConfig.getInstance().getCurrentPath(), "clob_" + collectObjInfo.getTaskID() + "_"
								+ Util.getDateString_yyyyMMddHHmmssSSS(collectObjInfo.getLastCollectTime()) + "_" + (clobIndex++) + ".clob");
						PrintWriter pw = new PrintWriter(clob);
						pw.print(colVal == null ? "" : colVal);
						pw.flush();
						pw.close();
						colVals.append(clob.getAbsolutePath()).append(DELIMIT);
						clobFiles.add(clob);
					}
				}
				++recordCount;

				// distribute(colVals.toString(), temP.getId(), table, sqlldrStore);

				if (sqlldrStore == null) {
					sqlldrStore = StoreFactory.getStore(temP.getId(), table, this.collectObjInfo,dataTime);
					sqlldrStore.open();
				}
				sqlldrStore.write(colVals.toString());
			}
			commit(sqlldrStore);
		} finally {
			CommonDB.close(destRs, descPs, destConn);
			
		}

		return recordCount;
	}

	/**
	 * @param temP
	 * 
	 *            设置特殊字段过滤html标签内容
	 */
	private void setHtmlTagsFilterColumnsSet(DBAutoTempletP2.Templet temP) {
		String htmlTagsFilterColumns = temP.getHtmlTagsFilterColumns();
		if (htmlTagsFilterColumns != null) {
			htmlTagsFilterColumnsSet = new HashSet<String>();
			String[] array = Util.split(htmlTagsFilterColumns, ",");
			for (String str : array) {
				htmlTagsFilterColumnsSet.add(str.toUpperCase());
			}
		}
	}

	/**
	 * 拼凑采集SQL语句
	 * 
	 * @param tableName
	 *            表名
	 * @param condition
	 *            where条件
	 * @return
	 */
	protected String toSql(String tableName, String condition) {
		String sql = null;
		if (Util.isNotNull(tableName)) {
			sql = "select * from " + tableName;
			if (Util.isNotNull(condition)) {
				condition = ConstDef.ParseFilePathForDB(condition, collectObjInfo.getLastCollectTime());
				sql += " where " + condition;
			}
		}
		return sql;
	}

	private void parseMeta(ResultSetMetaData srcMeta, ResultSetMetaData descMeta, Table table) throws Exception {
		int srcColCount = srcMeta.getColumnCount(); // 获取厂家表列数
		int destColCount = descMeta.getColumnCount(); // 获取采集表列数
		String tbName = table.getName(); // 采集表名
		Map<String, String> mcMap = null;
		// 注意: 元数据下标从1开始
		for (int j = 1; j <= destColCount; j++) {
			String destColName = descMeta.getColumnName(j);
			// 系统字段跳过
			if ("true".equals(is4G) && destColName.equalsIgnoreCase("MMEID"))
				continue;
			if (destColName.equalsIgnoreCase("OMCID") || destColName.equalsIgnoreCase("COLLECTTIME") || destColName.equalsIgnoreCase("STAMPTIME"))
				continue;

			int c = 0;
			for (int i = 1; i <= srcColCount; i++) {

				String srcColName = srcMeta.getColumnName(i);// 厂家表字段名称
				// 包含映射的情况
				if (mappingfields.containsKey(tbName) && mappingfields.get(tbName) != null && mappingfields.get(tbName).size() > 0) {
					mcMap = mappingfields.get(tbName);
					if (mcMap.containsKey(destColName) && 
							mcMap.get(destColName).equalsIgnoreCase(srcColName)) {
						putColumn(destColName, j, i, descMeta, table);
						break;
					}
				}
				// 不包含映射的情况
				if (srcColName.equalsIgnoreCase(destColName)) {
					putColumn(destColName, j, i, descMeta, table);
					break;
				}
 				c++;
			}
			// 厂家表没有该字段,设置索引为0
			if (c == srcColCount) {
				putColumn(destColName, j, 0, descMeta, table);
			}
		} // for end.
	}

	protected void putColumn(String colName, int index, int extIndex, ResultSetMetaData meta, Table table) throws SQLException {
		Column column = new Column();
		column.setIndex(index); // 此字段在采集表中的索引号
		column.setExtIndex(extIndex); // 此字段在厂家表中的索引号
		column.setName(colName); // 采集表字段名
		// column.setSrcType(srcMeta.getColumnType(extIndex));// 此字段在厂家表中对于的类型

		int type = meta.getColumnType(index); // 采集表中字段的类型
		int typeTmp = 1;

		if (type == 2) {
			typeTmp = 1;
			column.setFormat(String.valueOf(meta.getPrecision(index)));
		} else if (type == 12) {
			typeTmp = 2;
			column.setFormat(String.valueOf(meta.getPrecision(index)));
		} else if (type == 91 || type == 92 || type == 93) // 时间类型
		{
			typeTmp = 3;
			column.setFormat("YYYY-MM-DD HH24:MI:SS");
		} else if (type == 2005) {
			typeTmp = 4;
			column.setFormat("80000");
		} else
			column.setFormat(String.valueOf(meta.getPrecision(index)));

		column.setType(typeTmp);
		table.getColumns().put(index, column);
	}

	protected void distribute(String lineData, int templetId, Table tableD, AbstractStore<?> sqlldrStore) throws StoreException {
		tableD.setSplitSign(DELIMIT);
		// 处理sqlldrstore
		if (sqlldrStore == null) {
			sqlldrStore = StoreFactory.getStore(templetId, tableD, this.collectObjInfo,dataTime);
			// sqlldrStore = new SqlldrStore(new SqlldrStoreParam(templetId,
			// tableD));
			// sqlldrStore.setCollectInfo(collectObjInfo);
			// sqlldrStore.setTaskID(this.collectObjInfo.getTaskID());
			// sqlldrStore.setDataTime(this.collectObjInfo.getLastCollectTime());
			// sqlldrStore.setOmcID(this.collectObjInfo.getDevInfo().getOmcID());
			sqlldrStore.open();
		}
		sqlldrStore.write(lineData);
	}

	protected void commit(AbstractStore<?> sqlldrStore) {
		if (sqlldrStore != null) {
			try {
				sqlldrStore.flush();
				sqlldrStore.commit();
				sqlldrStore.close();
				sqlldrStore = null;

				for (File f : clobFiles) {
					f.delete();
				}
				clobFiles.clear();
			} catch (StoreException e) {
				log.error("sqlldrStore的处理中发生异常，原因：", e);
			}
		}
	}

	private String removeNoise(int colType, String colVal) {
		if (colVal == null)
			return "";

		// // 将日期类型截取yyyy-mm-dd hh:mi:ss

		if (colType == 3) {
			if (colVal.length() == 10) {
				return colVal.substring(0, 10) + " 00:00:00";
			}
			if (colVal.length() >= 19)
				return colVal.substring(0, 19);
		}
		// 字段中不能出现以下字符
		colVal = colVal.trim().replaceAll(";", " ").replaceAll("\r\n", " ");
		colVal = colVal.replaceAll("\n", " ").replaceAll("\r", " ");

		return colVal;
	}

	@Override
	public boolean parseData() throws Exception {
		throw new UnsupportedOperationException();
	}

	// 单元测试
	public static void main(String[] args) {
		CollectObjInfo obj = new CollectObjInfo(755123);
		DBAutoTempletP sect = new DBAutoTempletP();
		try {
			sect.parseTemp("clt_pm_alt_b10_gprs_parse.xml");
		} catch (Exception e1) {
			e1.printStackTrace();
		}

		DistributeTemplet dis = new DistributeTemplet();
		DevInfo dev = new DevInfo();
		dev.setOmcID(111);
		obj.setDevInfo(dev);
		obj.setParseTemplet(sect);
		obj.setDistributeTemplet(dis);
		obj.setLastCollectTime(new Timestamp(new Date().getTime()));

		DBAutoParser2 xml = new DBAutoParser2();
		xml.collectObjInfo = obj;

		xml.setFileName("D:\\RMFS00032.200");
		try {
			xml.parseData();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
