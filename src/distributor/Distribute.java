package distributor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import task.CollectObjInfo;
import util.CommonDB;
import util.LogMgr;
import util.Util;
import datalog.DataLogInfo;
import framework.ConstDef;
import framework.SystemConfig;

/**
 * 数据分发 基类
 * 
 * @author IGP TDT
 * @since 1.0
 */
public class Distribute {

	// //////////////////////////////////
	// 采集参数配置信息，分析程序需要根据参配置进行分析
	protected CollectObjInfo collectInfo;

	// 保存TableItem信息
	private Map<Integer, TableItem> tableItems = null;

	protected DistributeTemplet disTmp;// 发布模板信息

	private DistributeSqlLdr sqlldr = null;

	protected Logger log = LogMgr.getInstance().getSystemLogger();

	String fileName = "";

	// 分析模块的构造函数，用于初始化 TaskInfo
	public Distribute() {

	}

	// 分析模块的构造函数，用于初始化 TaskInfo
	public Distribute(CollectObjInfo TaskInfo) {
		try {
			this.collectInfo = TaskInfo;
			if (collectInfo.getCollectType() != 9) {
				disTmp = (DistributeTemplet) TaskInfo.getDistributeTemplet();
				init();// 初始化信息
			}
		} catch (Exception e) {
			log.error(TaskInfo.getTaskID() + " - 初始化出错", e);
		}
	}

	public void init(CollectObjInfo TaskInfo) {
		try {
			this.collectInfo = TaskInfo;
			if (collectInfo.getCollectType() != 9) {
				try {
					disTmp = (DistributeTemplet) TaskInfo.getDistributeTemplet();
				} catch (Exception e) {
					if (TaskInfo.getCollectType() != 9)
						log.error(TaskInfo.getTaskID() + " - 初始化模板时出错，请检查模板相关配置。", e);
				}
				init();// 初始化信息
			}
		} catch (Exception e) {
			log.error(TaskInfo.getTaskID() + " - 初始化出错", e);
		}
	}

	protected void init() {
		// 如果入库类型是逐条Insert的话，需要先创建SQL 信息
		// 利用sqlldr需要创建文件句柄
		TableItem tableItem = null;
		tableItems = ((DistributeTemplet) disTmp).tableItems;
		Map<Integer, DistributeTemplet.TableTemplet> tables = ((DistributeTemplet) this.disTmp).tableTemplets;
		// switch (((DistributeTemplet) this.disTmp).stockStyle)
		switch (disTmp.stockStyle) {
			case ConstDef.COLLECT_DISTRIBUTE_INSERT :// 利用SQL语句逐条插入
				for (int i = 0; i < tables.size(); i++) {
					tableItem = new TableItem();
					DistributeTemplet.TableTemplet tableTmp = tables.get(i);
					Map<Integer, DistributeTemplet.FieldTemplet> fields = tableTmp.fields;
					StringBuffer buffer = new StringBuffer();
					buffer.append("insert into " + tableTmp.tableName + "  values(");

					for (int j = 0; j < fields.size(); j++) {

						DistributeTemplet.FieldTemplet FieldInfo = fields.get(j);
						if (FieldInfo.m_nDataType == ConstDef.COLLECT_FIELD_DATATYPE_DATATIME) {
							buffer.append("to_date(?,'" + FieldInfo.m_strDataTimeFormat + "'),");
						} else {
							buffer.append("?,");
						}
					}
					String strSQL = buffer.substring(0, buffer.toString().length() - 1);
					strSQL += ")";
					tableItem.tableIndex = i;
					tableItem.sql = strSQL;
					tableItems.put(tableTmp.tableIndex, tableItem);
				}
				break;
			case ConstDef.COLLECT_DISTRIBUTE_FILE :
			case ConstDef.COLLECT_DISTRIBUTE_SQLLDR :
			case ConstDef.COLLECT_DISTRIBUTE_SQLLDR_DYNAMIC :
				String currentPath = SystemConfig.getInstance().getCurrentPath();
				for (int i = 0; i < tables.size(); i++) {
					tableItem = new TableItem();

					DistributeTemplet.TableTemplet TableInfo = tables.get(i);

					tableItem.tableIndex = TableInfo.tableIndex;

					Date now = new Date(collectInfo.getLastCollectTime().getTime());

					// SimpleDateFormat formatter = new
					// SimpleDateFormat("yyyyMMddHHmmss");
					// String strTime = formatter.format(now);
					String strTime = Util.getDateString_yyyyMMddHHmmss(now);

					String strFileName = collectInfo.getGroupId() + "_" + collectInfo.getTaskID() + "_" + strTime + "_" + i;

					// 保存文件名
					tableItem.fileName = strFileName;

					// 构建文件头
					String strTmpFileName = currentPath + File.separatorChar + strFileName + ".txt";
					buildFileHead(strTmpFileName, tableItem, TableInfo);

					tableItems.put(TableInfo.tableIndex, tableItem);
				}
				break;
		}
	}

	public void distribute(Object wParam, Object lParam) throws Exception {
		// do nothing
	}

	public void commit() {
		// do nothing
	}

	// 数据分析接口，接收从采集模块传送过来的数据进行分析，数据通过byte[]
	public boolean DistributeData(byte bData[], int tableIndex) {
		boolean bReturn = true;

		if (collectInfo == null || disTmp == null) {
			log.error("DistributeData: task 为 null. 数据分发失败.");
			collectInfo.log(DataLogInfo.STATUS_DIST, "DistributeData: task 为 null. 数据分发失败.");
			return false;
		}

		collectInfo.m_nAllRecordCount++;

		switch (disTmp.stockStyle) {
			case ConstDef.COLLECT_DISTRIBUTE_INSERT :// 利用SQL语句逐条插入
				Distribute_Insert(bData, tableIndex);
				break;
			case ConstDef.COLLECT_DISTRIBUTE_SQLLDR :// 利用SqlLdr批量导入
			case ConstDef.COLLECT_DISTRIBUTE_SQLLDR_DYNAMIC :
				bReturn = Distribute_Sqlldr(bData, tableIndex);
				break;
			case ConstDef.COLLECT_DISTRIBUTE_FILE :
				Distribute_File(bData);
				break;
		}

		return bReturn;
	}

	private void Distribute_File(byte[] bData) {
		int TmpType = 0;
		String logStr = null;
		try {
			TableItem tableItem = tableItems.get(TmpType);

			tableItem.recordCounts = tableItem.recordCounts + 1;

			FileWriter fw = tableItem.fileWriter;

			if (fw != null) {
				fw.write(new String(bData));
				fw.flush();
			} else {
				logStr = collectInfo.getSysName() + ": distribute error, no file create! ";
				log.error(logStr);
				collectInfo.log(DataLogInfo.STATUS_DIST, logStr);
			}
		} catch (Exception ex) {
			logStr = collectInfo.getSysName() + ": Distribute_File error.";
			log.error(logStr, ex);
			collectInfo.log(DataLogInfo.STATUS_DIST, logStr, ex);
			try {
				Thread.sleep(5 * 1000);
			} catch (InterruptedException e) {
			}
		}
	}

	// 以insert的方式保存数据
	public void Distribute_Insert(byte bData[], int tableIndex) {
		PreparedStatement stmt = null;
		Connection con = null;
		try {
			con = CommonDB.getConnection();

			TableItem tableItem = tableItems.get(tableIndex);
			String strSQL = tableItem.sql;

			stmt = con.prepareStatement(strSQL);
			String m_strData = new String(bData);
			// 将获得的数据分行，以"\n"来分行
			String[] strRowData = m_strData.split("\n");
			// 循环加载数据到PreparedStatement中
			for (int i = 0; i < strRowData.length; i++) {
				// 利用";"将数据分列
				String[] strColData = strRowData[i].split(";");
				for (int j = 0; j < strColData.length; j++) {
					// 不管是什么数据类型，现在统一转换成String 插入到数据库中
					stmt.setString(j + 1, strColData[j]);
				}
				stmt.addBatch();
			}
			stmt.executeBatch();
			con.commit();
		} catch (Exception e) {
			log.error("Distribute_Insert error.", e);
			collectInfo.log(DataLogInfo.STATUS_DIST, "Distribute_Insert error.");
		} finally {
			try {
				if (stmt != null)
					stmt.close();
				if (con != null)
					con.close();
			} catch (Exception sqle) {
			}
		}
	}

	/**
	 * 在cols中寻找所有的col，返回它们的索引位置。 如果只找到1个col，返回null，表示col在cols中未重复
	 * 
	 * @param cols
	 * @param col
	 * @return
	 */
	private List<Integer> findDuplicateCol(List<String> cols, String col) {
		List<Integer> index = new ArrayList<Integer>();
		for (int i = 0; i < cols.size(); i++) {
			if (cols.get(i).equalsIgnoreCase(col)) {
				index.add(i);
			}
		}
		return index.size() > 1 ? index : null;
	}

	// 以Sqlldr 方式创建临时文件
	public boolean Distribute_Sqlldr(byte bData[], int tableIndex) {
		boolean bReturn = true;
		String logStr = null;
		try {
			if (tableItems == null) {
				logStr = collectInfo.getSysName() + ": Distribute_Sqlldr: m_hFile 为null,数据分发失败. 请检查模板配置.";
				log.error(logStr);
				collectInfo.log(DataLogInfo.STATUS_END, logStr);
				return false;
			}

			TableItem tableItem = tableItems.get(tableIndex);
			if (tableItem == null) {
				logStr = collectInfo.getSysName() + ": Distribute_Sqlldr: tableItem 为null,数据分发失败. 请检查模板配置.";
				log.error(logStr);
				collectInfo.log(DataLogInfo.STATUS_END, logStr);
				return false;
			}

			tableItem.recordCounts = tableItem.recordCounts + 1;

			FileWriter fw = tableItem.fileWriter;

			File txt = new File(SystemConfig.getInstance().getCurrentPath(), tableItem.fileName + ".txt");
			List<String> raws = null;
			Map<String, List<Integer>> colToIndex = new HashMap<String, List<Integer>>();
			List<Integer> dels = new ArrayList<Integer>();
			String splitSign = ";";
			if (tableItem.head == null && txt.length() > 0) {
				BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(txt)));
				String firstLine = br.readLine();
				br.close();
				try {
					String omcId = String.valueOf(collectInfo.getDevInfo().getOmcID());
					splitSign = firstLine.substring(firstLine.indexOf(omcId) + omcId.length(), firstLine.indexOf(omcId) + omcId.length() + 1);
				} catch (Exception e) {
					logStr = "get splitSign error:" + e.getMessage();
					log.error(logStr);
					collectInfo.log(DataLogInfo.STATUS_DIST, logStr, e);
				}
				String[] items = firstLine.split(";");
				raws = new ArrayList<String>();
				for (String s : items) {
					if (Util.isNotNull(s)) {
						raws.add(s.trim());
					}
				}
				tableItem.head = raws;
				for (int i = 0; i < raws.size(); i++) {
					String r = raws.get(i);
					if (!colToIndex.containsKey(r)) {
						List<Integer> index = findDuplicateCol(raws, r);
						if (index != null) {
							colToIndex.put(r, index);
						}
					}
				}
				Collection<List<Integer>> c = colToIndex.values();
				for (List<Integer> list : c) {
					for (Integer i : list) {
						dels.add(i);
					}
				}
			}

			if (fw != null) {
				String data = new String(bData);
				if (raws != null) {
					String[] items = data.split(splitSign);
					// 去重复列
					StringBuilder sb = new StringBuilder();
					for (int i = 0; i < items.length; i++) {
						boolean flag = false;
						for (Integer del : dels) {
							if (i == del) {
								flag = true;
							}
						}
						if (!flag) {
							sb.append(items[i]).append(splitSign);
						}
					}
					sb.deleteCharAt(sb.length() - 1);
					data = sb.toString();
				}

				// 去掉包含列头的行。
				File f = new File(SystemConfig.getInstance().getCurrentPath(), tableItem.fileName + ".txt");
				if (f.length() > 0) {
					BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(txt)));
					String s = br.readLine();
					br.close();
					if(s != null){
						List<String> rs = new ArrayList<String>();
						String[] ss = s.split(splitSign);
						for (String str : ss) {
							if (Util.isNotNull(str)) {
								rs.add(str.trim());
							}
						}
						if (rs.size() > 3 && data.split(splitSign).length > 3 && rs.get(3).equalsIgnoreCase(data.split(splitSign)[3])) {
							data = "";
						}
					}
				}

				fw.write(data);
				// fw.write(strBuff.toString());
				fw.flush();
			} else {
				log.debug("FileWriter ID" + tableIndex);
				log.debug("FileWriter fw " + fw);
				log.debug("containsKey" + tableItems.containsKey(tableIndex));
				log.debug("m_hFile " + tableItems.size());

				mySleep(5 * 1000);
			}
			int nOnceShockCount = ((DistributeTemplet) disTmp).onceStockCount;
			// nOnceShockCount 为-1 表示下载完后一起导入
			if (nOnceShockCount != -1 && tableItem.recordCounts >= nOnceShockCount) {
				// 调用sqlldr批量导入
				if (sqlldr == null)
					sqlldr = new DistributeSqlLdr(collectInfo);
				DistributeTemplet distmp = (DistributeTemplet) collectInfo.getDistributeTemplet();
				DistributeTemplet.TableTemplet table = distmp.tableTemplets.get(tableIndex);
				String strOldFileName = tableItem.fileName;

				fw.close();// 关闭旧文件,创建新文件,避免打开的文件过多
				sqlldr.buildSqlLdr(table.tableIndex, strOldFileName);

				tableItem.recordCounts = 0;
				String strCurrentPath = SystemConfig.getInstance().getCurrentPath();

				Date now = new Date(collectInfo.getLastCollectTime().getTime());
				SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");
				String strTime = formatter.format(now);

				String strNewFileName = collectInfo.getGroupId() + "_" + collectInfo.getTaskID() + "_" + strTime + "_" + String.valueOf(tableIndex);

				tableItem.fileName = strNewFileName;

				// 构建文件头
				String strTmpFileName = strCurrentPath + File.separatorChar + strNewFileName + ".txt";
				buildFileHead(strTmpFileName, tableItem, table);
			}
		} catch (Exception e) {
			bReturn = false;
			log.error("Distribute_Sqlldr error.", e);
			collectInfo.log(DataLogInfo.STATUS_DIST, "Distribute_Sqlldr error.", e);
			mySleep(5 * 1000);
		}

		return bReturn;
	}

	/**
	 * 构建数据文件的头
	 * 
	 * @param strFileName
	 * @param tableItem
	 * @param table
	 * @return
	 */
	private boolean buildFileHead(String strFileName, TableItem tableItem, DistributeTemplet.TableTemplet table) {
		if (strFileName == null || strFileName.equals(""))
			return false;

		String logStr = null;

		boolean bReturn = true;

		FileWriter fw = null;

		try {
			fw = new FileWriter(strFileName);
		} catch (IOException e) {
			logStr = collectInfo.getSysName() + ": error when building file head. ";
			log.error(logStr, e);
			collectInfo.log(DataLogInfo.STATUS_DIST, logStr, e);
			return false;
//		} finally{
//			IOUtils.closeQuietly(fw);
		}

		tableItem.fileWriter = fw;

		if (table.isFillTitle) {
			try {
				int len = table.fields.size();
				for (int k = 0; k < len - 1; k++) {
					DistributeTemplet.FieldTemplet field = table.fields.get(k);

					fw.write(field.m_strFieldName + ";");
				}
				fw.write(table.fields.get(len - 1).m_strFieldName);

				fw.write("\n");
				fw.flush();
			} catch (IOException e) {
				logStr = collectInfo.getSysName() + ": error when building file head. ";
				log.error(logStr, e);
				collectInfo.log(DataLogInfo.STATUS_DIST, logStr, e);
				bReturn = false;
			} finally {
				// TODO fw.close();
			}
		}

		return bReturn;
	}

	/** 休眠方法 */
	private void mySleep(long ms) {
		try {
			Thread.sleep(ms);
		} catch (InterruptedException e) {
		}
	}

	public CollectObjInfo getCollectInfo() {
		return collectInfo;
	}

	public void setCollectInfo(CollectObjInfo collectInfo) {
		this.collectInfo = collectInfo;
	}

}
