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
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import parser.Parser;
import task.CollectObjInfo;
import task.DevInfo;
import templet.zte.PmTempletP;
import util.DbPool;
import util.LogMgr;
import util.Util;

/**
 * 中兴文件接口sqlldr入库解析 2010-02-02
 * 
 * @author liuwx
 */
public class CV2ASCII extends Parser {

	private static String END_FILE_SIGN = "<End of the ZTE_BSSB_OMC File>";

	private String tableSign;

	private static final String METADATASQL = "select column_name,data_type,data_length from user_tab_columns where upper(table_name)= ?";

	private List<String> TABLES = new ArrayList<String>();

	// 存储表模板信息
	public Map<Integer, PmTempletP.TableTemplet> tableInfo = new HashMap<Integer, PmTempletP.TableTemplet>();

	private static Logger log = LogMgr.getInstance().getSystemLogger();
	
	/**
	 * 主要是缓存表结构，避免消耗过多的数据库资源
	 */
	private  static  Map<String,List<String>>  tableInfoMap=new HashMap<String,List<String>> ();

	public String tmpFileName = null;

	public CV2ASCII() {
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

	/**
	 * 解析文件并入库
	 * 
	 * @param in
	 * @param tempFile
	 */
	public boolean distribute(InputStream in) {

		int omcId = collectObjInfo.getDevInfo().getOmcID();
		Timestamp stamptime = collectObjInfo.getLastCollectTime();
		String beginTime = null;
		BufferedReader reader = null;
		InputStreamReader read = new InputStreamReader(in);
		try {
			reader = new BufferedReader(read);
			String lineData = null;
			boolean flag = false;
			boolean skipFlag = flag;

			PmTempletP.HeaderTemplet headerTemplet = new PmTempletP().new HeaderTemplet();
			String tableName = null;
			String tableDesc = null;

			String fieldSplit = null;
			String valueSplit = null;

			int tableIndex = 0;

			List<String> headList = null;

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

				beginTime = lineData.substring(lineData.indexOf("StartTime:") + 10, lineData.indexOf("\tEndTime"));

			}
			lineData = reader.readLine();
			if (lineData.contains("Ver")) {
				String lineArray[] = lineData.split(":");
				headerTemplet.ver = lineArray[1];
			}

			boolean tableFlag = false;
			boolean headerFlag = false;
			// boolean dataFlag = false;
			PmTempletP.TableTemplet tableTemplet = null;

			SqlldrParam param = new SqlldrParam();
			List<Field> fieldList = new ArrayList<Field>();
			Map<String, ArrayList<String>> records = new HashMap<String, ArrayList<String>>();
			StringBuilder sbHead = new StringBuilder();
			int count = 0;
			StringBuilder headTxt = new StringBuilder();
			reader.readLine();
			while ((lineData = reader.readLine()) != null) {
				lineData = lineData.trim();

				try {

					if (lineData.contains(END_FILE_SIGN)) {

						// 文件结束的时候，将最后一个表的数据进行提交
						if (Util.isNotNull(tableName) && !TABLES.contains(tableName)) {
							TABLES.add(tableName);
							if (sbHead.toString().equals("")) {
								continue;
							}
							param.taskID = collectObjInfo.getTaskID();
							param.tbName = tableName;
							param.omcID = omcId;
							param.records = records;
							param.commonFields = fieldList;
							param.dataTime = stamptime;
							param.head = sbHead.delete(sbHead.length() - 1, sbHead.length()).toString();
							param.headTxt = headTxt.delete(headTxt.length() - 1, headTxt.length()).toString();
							param.beginTime = beginTime;
							param.tableIndex = tableIndex;

							PmSqlldrTool sqlldr = new PmSqlldrTool(param, collectObjInfo);
							sqlldr.execute();
							param.clear();

							tableFlag = false;
							headerFlag = false;
							// dataFlag = false;
							sbHead.delete(0, sbHead.length());
							headTxt.delete(0, headTxt.length());
							tableName = null;
							records.clear();

							log.info("Task-" + collectObjInfo.getTaskID() + " 文件名" + this.getFileName() + "成功解析完成。");
						}

						break;
					}
					if (isContainTableTag(lineData)) {

						// 开始获得表的数据
						if (Util.isNotNull(tableName) && !TABLES.contains(tableName) || lineData.contains(END_FILE_SIGN)) {
							TABLES.add(tableName);
							if (sbHead.toString().equals("")) {
								continue;
							}
							param.taskID = collectObjInfo.getTaskID();
							param.tbName = tableName;
							param.omcID = omcId;
							param.records = records;
							param.commonFields = fieldList;
							param.dataTime = stamptime;
							param.head = sbHead.delete(sbHead.length() - 1, sbHead.length()).toString();
							param.headTxt = headTxt.delete(headTxt.length() - 1, headTxt.length()).toString();
							param.beginTime = beginTime;
							param.tableIndex = tableIndex;

							PmSqlldrTool sqlldr = new PmSqlldrTool(param, collectObjInfo);
							sqlldr.execute();
							param.clear();

							tableFlag = false;
							headerFlag = false;
							// dataFlag = false;
							sbHead.delete(0, sbHead.length());
							headTxt.delete(0, headTxt.length());
							tableName = null;
							records.clear();
						}
						for (int i = 0; i < this.tableInfo.size(); i++) {
							tableTemplet = this.tableInfo.get(i);
							tableIndex = tableTemplet.tableIndex;
							tableName = tableTemplet.strTableName.toUpperCase();
							tableDesc = tableTemplet.table_desc;

							fieldSplit = tableTemplet.fieldSplit;
							valueSplit = tableTemplet.valueSplit;

							// <TABLEDESC>POName=1X:
							// 全局目标BSS切换数据话务量对象||POName=1X:
							// 全局数据切换话务量对象(作为目标BSS)</TABLEDESC>
							// POName=1X: 全局目标BSS切换数据话务量对象
							// 找不到对应的表对应关系时，则 找 POName=1X:
							// 全局数据切换话务量对象(作为目标BSS)对应的表关系
							// add on 2011-03-30
							// 修改目的：便于模板统一，如果厂商文件类型表对应关系发生改变，只需要修改模板的表对应关系即可(用||分隔)这样不会出现多个模板版本的情况，便于维护管理
							String[] tDesc = null;
							if (Util.isNotNull(tableDesc)) {
								tDesc = tableDesc.split("\\|\\|");
							}
							if (tDesc != null) {
								for (String s : tDesc) {
									if (Util.isNull(s))
										continue;
									s = s.trim();
									skipFlag = lineData.equals(s);
									if (skipFlag)
										break;
								}
							}

							// end add
							// skipFlag = lineData.equals(tableDesc.trim());
							if (skipFlag) {
								tableFlag = true;
								break;
							} else
								tableName = null;
						}
						continue;

					} else {
						if (!tableFlag)
							continue;

						// 从文件取得每一行数据（可能是表头，也可能是数据）
						String str[] = lineData.split("\t");
						// 用于存放数据分割后在数据库中存在的索引

						// add on 2011-04-13
						// 先用"\t"分割，如果长度小于等于1，则尝试用空格\\s+分割
						if (str == null || str.length <= 1)
							str = lineData.split("\\s+");
						// end add

						if (!headerFlag) {

							tableName = tableTemplet.strTableName;
							tableIndex = tableTemplet.tableIndex;

							// 获取表头列
							List<String> columns = getMetaData(tableName);

							if (columns == null || columns.size() <= 0) {
								tableFlag = false;
								headerFlag = false;
								// dataFlag = false;
								log.error(tableName + "表不存在" + " 表编号 " + tableIndex);
								continue;
							}

							// 将索引添加到集合中
							int size = str.length;

							String head = null;
							headList = new ArrayList<String>();
							// 初始化list
							for (int k = 0; k < size; k++)
								headList.add(null);

							for (int i = 0; i < str.length; i++) {
								head = str[i].toUpperCase();
								if (str[i] != null && columns.contains(head)) {
									sbHead.append(head).append(",");
									headTxt.append(head).append(";");
									headList.set(i, head);
								}
							}
							headerFlag = true;
							continue;
						} else {
							if (headerFlag) {
								if (headList == null) {
									tableFlag = false;
									continue;
								}

								ArrayList<String> list = new ArrayList<String>();

								for (int k = 0; k < str.length; k++) {
									for (int j = 0; j < headList.size(); j++) {
										if (headList.get(j) == null)
											continue;
										if (j == k) {
											if (str[k] != null) {
												list.add(str[k]);
											}
										}
									}
								}
								count++;
								records.put(tableName + "_" + count, list);
							}

						}
					}

				} catch (Exception e) {
					log.error("Task-" + collectObjInfo.getTaskID() + " 表名 " + tableName + "  出现异常    ", e);
				}
			}

			read.close();
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

		} finally {
			try {
				if (read != null)
					read.close();
				if (reader != null)
					reader.close();
				if (in != null)
					in.close();
			} catch (IOException e) {
				log.error(collectObjInfo + "关闭文件出现异常,原因：", e);
			}

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
		if (file.length() <= 0) {
			log.error(collectObjInfo + ": 文件" + path + "长度为0，请确定是否下载成功，ftp文件内容为空");
			return false;
		}
		PmTempletP tem = (PmTempletP) collectObjInfo.getParseTemplet();
		// String tempFile = tem.tmpFileName;
		tableSign = tem.getTableSignString();
		tableInfo = tem.tableInfo;
		InputStream in = null;
		try {
			in = new FileInputStream(file);
			flag = distribute(in);
		} catch (FileNotFoundException e) {
			log.error(collectObjInfo + ": 文件" + path + "不存在。");
			return false;
		} finally {
			if (in != null) {
				try {
					in.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			// liangww add 2012-10-29 解析完文件，得清除tables,解决无法解析多个文件的bug
			this.TABLES.clear();
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

	public String listToString(List<String> fields, String splitSign) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < fields.size() - 1; i++) {
			String value = fields.get(i);
			sb.append(value).append(splitSign);
		}
		sb.append(fields.get(fields.size() - 1));

		return sb.toString();
	}

	// 单元测试
	public static void main(String[] args) {
		CollectObjInfo obj = new CollectObjInfo(755123);
		DevInfo dev = new DevInfo();
		dev.setOmcID(111);
		obj.setDevInfo(dev);
		obj.setLastCollectTime(new Timestamp(new Date().getTime()));

		PmTempletP tem = new PmTempletP();
		tem.parseTemp("clt_cm_zte_bssb_honbcell_parse.xml");
		obj.setParseTemplet(tem);

		CV2ASCII ztepm = new CV2ASCII();
		ztepm.collectObjInfo = obj;
		ztepm.setFileName("F:\\ftproot\\199700_NBHO_201104100000.txt");
		ztepm.parseData();
	}

}
