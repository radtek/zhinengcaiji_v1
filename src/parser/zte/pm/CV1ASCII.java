package parser.zte.pm;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import parser.Parser;
import templet.zte.PmTempletP;
import util.CommonDB;
import util.DBLogger;
import util.DbPool;
import util.LogMgr;
import util.Util;

/**
 * 中兴多表文件解析 2010-02-02
 * 
 * @author liuwx
 */
public class CV1ASCII extends Parser {

	private static String END_FILE_SIGN = "<End of the ZTE_BSSB_OMC File>";

	// private List<String> tableSignList;
	private String tableSign;

	private static final String METADATASQL = "select column_name,data_type,data_length from user_tab_columns where upper(table_name)= ?";

	private List<String> TABLES = new ArrayList<String>();

	// private CollectObjInfo collectObjInfo;
	
	/**
	 * 主要是缓存表结构，避免消耗过多的数据库资源
	 */
	private  static  Map<String,List<String>>  tableInfoMap=new HashMap<String,List<String>> ();
	

	private static DBLogger dbLogger = LogMgr.getInstance().getDBLogger();

	/* 入库信息 */
	public int tmpID = 0;// 模板类型

	public String strTmpName = null;// 模板名称

	public String strEdition = null;// 版本类型

	/* 数据库信息 */
	public String dbDriver = ""; // 连接数据库的驱动

	public String dbDriverUrl = "";// 数据库的连接信息

	public String dbDataBase = "";// 数据库名称

	public String dbUserName = "";// 用户名

	public String dbPassword = "";// 密码

	// 存储表模板信息
	public Map<Integer, PmTempletP.TableTemplet> tableInfo = new HashMap<Integer, PmTempletP.TableTemplet>();

	private static Logger log = LogMgr.getInstance().getSystemLogger();

	public String tmpFileName = null;

	public CV1ASCII() {
	}

	/**
	 * 取得数据库中对应表的元数据， 参数表名
	 * 
	 * @param tableName
	 * @return
	 */
	public synchronized List<String> getMetaData(String tableName) {
		if(tableInfoMap.containsKey(tableName))
			return tableInfoMap.get(tableName);
		
		Connection connection = DbPool.getConn();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		List<String> list = new ArrayList<String>();
		try {
			stmt = connection.prepareStatement(METADATASQL);
			stmt.setString(1, tableName.toUpperCase());
			rs = stmt.executeQuery();
			while (rs.next()) {
				list.add(rs.getString(1));
			}
			rs.close();
			stmt.close();
		} catch (Exception e) {
			log.error(collectObjInfo + ": 获取元数据时出现异常.", e);
		} finally {
			try {
				if (stmt != null) {
					stmt.close();
				}
				if (connection != null) {
					connection.close();
				}
			} catch (Exception e) {
			}
		}
		return list;
	}

	// /**
	// * 判断是否是表标识符
	// *
	// * @param lineData
	// * @return
	// */
	// public boolean isTableSign(String lineData)
	// {
	// boolean flag = false;
	// if ( tableSignList != null )
	// {
	// for (String value : tableSignList)
	// {
	// if ( lineData.contains(value) )
	// {
	// flag = true;
	// break;
	// }
	// }
	// }
	// return flag;
	// }

	/**
	 * 解析文件并入库
	 * 
	 * @param in
	 * @param tempFile
	 */
	public boolean distribute(InputStream in) {

		int omcId = collectObjInfo.getDevInfo().getOmcID();
		String dateTemp = null;

		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(in));
			String lineData = null;
			boolean flag = false;
			boolean skipFlag = flag;
			String beginDate = null;

			PmTempletP.HeaderTemplet headerTemplet = new PmTempletP().new HeaderTemplet();
			// int headerCount = 0;
			String tableName = null;
			String tableDesc = null;

			List<PmTempletP.TableTemplet> moreTable = null;
			boolean moreTableFlag = false;
			int tableIndex = 0;
			boolean tableField = false;
			StringBuffer tableHeader = null;
			List<StringBuffer> sbList = null;
			List<String> sqlList = new ArrayList<String>();
			Map<Integer, String> valueMap = null;
			List<Map<Integer, String>> moreTableValueMapList = null;
			List<StringBuffer> tableColumnList = null;
			StringBuffer sqlBf = new StringBuffer();

			lineData = reader.readLine();
			lineData = reader.readLine();
			if (lineData.contains("NEType")) {
				String lineArray[] = lineData.split(":");
				headerTemplet.netype = lineArray[1].trim();
			}
			lineData = reader.readLine();
			if (lineData.contains("NodeId")) {
				String lineArray[] = lineData.split(":");
				headerTemplet.nodeid = lineArray[1];
			}
			lineData = reader.readLine();
			if (lineData.contains("StartTime") && lineData.contains("EndTime")) {
				lineData = lineData.trim();

				beginDate = lineData.substring(lineData.indexOf("StartTime:") + 10, lineData.indexOf("\tEndTime"));

				dateTemp = beginDate;
			}
			lineData = reader.readLine();
			if (lineData.contains("Ver")) {
				String lineArray[] = lineData.split(":");
				headerTemplet.ver = lineArray[1];
			}

			while ((lineData = reader.readLine()) != null) {
				lineData = lineData.trim();

				if (isContainTableTag(lineData)) {
					// 开始获得表的数据
					if (Util.isNotNull(tableName) && !TABLES.contains(tableName)) {
						TABLES.add(tableName);
						CommonDB.executeBatch(sqlList);
						dbLogger.log(omcId, tableName, Util.getDate1(dateTemp).getTime(), sqlList.size(), collectObjInfo.getTaskID());
						log.info(collectObjInfo.getTaskID() + "表索引：" + tableIndex + "  表: " + tableName + "表中入库了" + sqlList.size() + "条数据");
						sqlList.clear();
					}
					sqlList = new ArrayList<String>();
					moreTable = new ArrayList<PmTempletP.TableTemplet>();

					for (int i = 0; i < this.tableInfo.size(); i++) {
						PmTempletP.TableTemplet TableTemplet = this.tableInfo.get(i);
						tableIndex = TableTemplet.tableIndex;
						tableName = TableTemplet.strTableName;
						tableDesc = TableTemplet.table_desc;

						skipFlag = lineData.equals(tableDesc.trim());
						if (skipFlag) {
							moreTable.add(TableTemplet);
						}
					}
					if (moreTable.size() > 0) {
						moreTableFlag = true;
						skipFlag = true;
						flag = true;
					} else {
						tableName = null;
					}
				} else {
					if (moreTable != null) {
						if (moreTableFlag) {
							StringBuffer tableColumn = null;
							// 从文件取得每一行数据（可能是表头，也可能是数据）
							String str[] = lineData.split("\t");
							// 用于存放数据分割后在数据库中存在的索引
							List<String> lists = new ArrayList<String>();
							StringBuffer sbTemp = new StringBuffer("insert into ");
							if (flag) {
								if (!skipFlag) {
									continue;
								}
								sbList = new ArrayList<StringBuffer>();
								moreTableValueMapList = new ArrayList<Map<Integer, String>>();
								tableColumnList = new ArrayList<StringBuffer>();
								for (PmTempletP.TableTemplet templet : moreTable) {
									tableName = templet.strTableName;
									tableIndex = templet.tableIndex;

									tableColumn = new StringBuffer();
									tableColumnList.add(tableColumn);

									// 获取表头列
									List<String> columns = getMetaData(tableName);
									tableHeader = new StringBuffer();
									tableHeader.append("OMCID,");
									tableHeader.append("COLLECTTIME,");
									tableHeader.append("STAMPTIME,");
									tableHeader.append("BEGINTIME,");

									// 将索引添加到集合中
									valueMap = new HashMap<Integer, String>();
									for (int i = 0; i < str.length; i++) {
										if (str[i] != null && columns.contains(str[i].toUpperCase())) {
											lists.add(str[i].toUpperCase());
											valueMap.put(i, str[i].toUpperCase());
										}
									}
									for (int kk = 0; kk < valueMap.size(); kk++) {
										String value = valueMap.get(kk);
										if (value != null) {
											if (kk < valueMap.size() - 1) {
												tableHeader.append(value + ",");
											} else {
												tableHeader.append(value);
											}
										}
									}
									sbList.add(tableHeader);
									moreTableValueMapList.add(valueMap);
									// flag = false;
									// tableField = true;
								}
								flag = false;
								tableField = true;
							} else {
								if (tableField) {
									if (!skipFlag) {
										continue;
									}
									for (int mm = 0; mm < moreTable.size(); mm++) {
										tableHeader = sbList.get(mm);
										PmTempletP.TableTemplet tem = moreTable.get(mm);
										tableName = tem.strTableName;

										valueMap = moreTableValueMapList.get(mm);

										sqlBf.append(sbTemp);
										sqlBf.append(tableName + " (");

										tableColumn = tableColumnList.get(mm);

										sqlBf.append(tableHeader);
										tableColumn.append(") values (");
										tableColumn.append(omcId + ",");
										tableColumn.append("sysdate,");// 'yyyy-MM-dd
										// HH24:mi:ss'
										tableColumn.append("to_date('" + dateTemp + "','yyyy-MM-dd HH24:mi:ss'),");
										tableColumn.append("to_date('" + beginDate + "','yyyy-MM-dd HH24:mi:ss'),");
										for (int k = 0; k < str.length; k++) {
											Set<Entry<Integer, String>> setEntry = valueMap.entrySet();
											Iterator<Entry<Integer, String>> itt = setEntry.iterator();

											while (itt.hasNext()) {
												Entry<Integer, String> obj = itt.next();
												if (k == obj.getKey()) {
													if (k < setEntry.size() - 1) {
														tableColumn.append(str[k] + ",");
													} else {
														tableColumn.append(str[k]);
													}
												}
											}
										}
										tableColumn.append(")\r\n");
										sqlBf.append(tableColumn);

										sqlList.add(sqlBf.toString());

										tableColumn.delete(0, tableColumn.length());
										sqlBf.delete(0, sqlBf.length());
									}
								}
							}
							if (lineData.contains(END_FILE_SIGN)) {
								log.info(collectObjInfo + "成功解析完成。");
								break;
							}
						}
					}
				}
			}

			reader.close();
			in.close();

			// fw.flush();
			// fw.close();
		} catch (FileNotFoundException e) {
			log.error(collectObjInfo + "文件没有找到,原因：", e);
			return false;
		} catch (IOException e) {
			log.error(collectObjInfo + "出现IO异常,原因：", e);
			return false;
		} catch (Exception e) {
			log.error(collectObjInfo + "解析时出现异常,原因：", e);
			return false;

		}
		return true;
	}

	@Override
	public boolean parseData() {
		boolean flag = false;
		String path = this.getFileName();
		File file = new File(path);
		if (!file.exists()) {
			log.error(collectObjInfo + ": 文件" + path + "不存在。");
			return false;
		}

		PmTempletP tem = (PmTempletP) collectObjInfo.getParseTemplet();
		// String tempFile = tem.tmpFileName;
		tableSign = tem.getTableSignString();
		tableInfo = tem.tableInfo;

		try {
			InputStream in = new FileInputStream(new File(path));
			flag = distribute(in);
		} catch (FileNotFoundException e) {
			log.error(collectObjInfo + ": 文件" + path + "不存在。");
			return false;
		}

		return flag;
	}

	public boolean isContainTableTag(String lineData) {
		if (lineData == null || "".equals(lineData.trim()))
			return false;
		int i = lineData.indexOf(":");
		if (i == -1)
			return false;
		return tableSign.contains(lineData.substring(0, i + 1));

	}
}
