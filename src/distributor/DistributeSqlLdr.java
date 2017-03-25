package distributor;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import task.CollectObjInfo;
import util.BalySqlloadThread;
import util.CommonDB;
import util.DBLogger;
import util.LogMgr;
import util.SqlldrResult;
import util.Util;
import util.loganalyzer.SqlLdrLogAnalyzer;
import util.string.LevenshteinDistance;
import alarm.AlarmMgr;
import datalog.DataLogInfo;
import distributor.DistributeTemplet.FieldTemplet;
import framework.ConstDef;
import framework.SystemConfig;

public class DistributeSqlLdr {

	private CollectObjInfo collectInfo;

	private DistributeTemplet disTmp;

	private Logger log = LogMgr.getInstance().getSystemLogger();

	private static DBLogger dbLogger = LogMgr.getInstance().getDBLogger();

	public DistributeSqlLdr(CollectObjInfo ColInfo) {
		this.collectInfo = ColInfo;
		disTmp = (DistributeTemplet) this.collectInfo.getDistributeTemplet();
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

	// 建立Sqlldr信息文件,并运行
	public void buildSqlLdr(int tableIndex, String tempFile) {
		String logStr = null;
		DistributeTemplet.TableTemplet tableInfo = disTmp.tableTemplets.get(tableIndex);
		// 保存临时文件的路径
		String currentPath = SystemConfig.getInstance().getCurrentPath();
		String charSet = SystemConfig.getInstance().getSqlldrCharset();

		int retCode = -1;

		File txttempfile = new File(currentPath, tempFile + ".txt");
		if (!txttempfile.exists()) {
			logStr = collectInfo.getSysName() + ": " + txttempfile.getAbsolutePath() + " 不存在.";
			log.debug(logStr);
			collectInfo.log(DataLogInfo.STATUS_DIST, logStr);
			return;
		}

		if (txttempfile.length() == 0) {
			logStr = collectInfo.getSysName() + ": " + txttempfile.getAbsolutePath() + " 内容为空.";
			log.debug(logStr);
			collectInfo.log(DataLogInfo.STATUS_DIST, logStr);
			txttempfile.delete();
			return;
		}

		// By xumg start
		BufferedWriter bw = null;
		try {
			bw = new BufferedWriter(new FileWriter(currentPath + File.separatorChar + tempFile + ".ctl", false));

			File txtFile = new File(currentPath + File.separatorChar + tempFile + ".txt");
			InputStream in = new FileInputStream(txtFile);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String splitSign = ";";
			String firstLine = br.readLine();
			try {
				String omcId = String.valueOf(collectInfo.getDevInfo().getOmcID());
				splitSign = firstLine.substring(firstLine.indexOf(omcId) + omcId.length(), firstLine.indexOf(omcId) + omcId.length() + 1);
			} catch (Exception e) {
				logStr = "get splitSign error:" + e.getMessage();
				log.error(logStr);
				collectInfo.log(DataLogInfo.STATUS_DIST, logStr, e);
			}
			if (br != null) {
				br.close();
			}
			if (in != null) {
				in.close();
			}
			List<String> raws = new ArrayList<String>();
			Map<String, List<Integer>> colnameToIndex = new HashMap<String, List<Integer>>();
			if (firstLine != null) {
				String[] items = firstLine.split(";");
				for (String s : items) {
					if (Util.isNotNull(s)) {
						raws.add(s.trim());
					}
				}

				// chensj 2010-07-15
				// 原始文件列头有重复时，重复的列都不入库，例如有2个名字一样的列，无法确定哪列才是正确的，所以这两列都不要。
				for (String s : raws) {
					if (!colnameToIndex.containsKey(s)) {
						List<Integer> index = findDuplicateCol(raws, s);
						if (index != null) {
							colnameToIndex.put(s, index);
						}
					}
				}
			}

			if (Util.isOracle()) {
				// bw.write("unrecoverable\r\n");
				bw.write("load data\r\n");

				if (charSet != null && charSet.length() > 0)
					bw.write("CHARACTERSET " + charSet + " \r\n");
				else
					bw.write("CHARACTERSET AL32UTF8 \r\n");

				bw.write("infile '" + currentPath + File.separatorChar + tempFile + ".txt' ");
				bw.write("append into table " + tableInfo.tableName + " \r\n");
				bw.write("FIELDS TERMINATED BY \";\"\r\n");
				bw.write("TRAILING NULLCOLS\r\n");
				bw.write("(");

				if (disTmp.stockStyle == ConstDef.COLLECT_DISTRIBUTE_SQLLDR_DYNAMIC) {
					String m_RawColumnsList = ReadFileFirstLine(tempFile);
					if (m_RawColumnsList == null)
						return;

					// 字段的映射名
					String[] FieldMappingList = m_RawColumnsList.split(";");
					String StrNewFieldList = "";
					for (int i = 0; i < FieldMappingList.length; i++) {
						// 获取该隐射字段的名称
						String strFieldMap = FieldMappingList[i].trim();
						if (strFieldMap.trim() == "")
							continue;

						for (int j = 0; j < tableInfo.fields.size(); j++) {
							DistributeTemplet.FieldTemplet field = tableInfo.fields.get(j);
							if (field.m_strFieldMapping.equals(strFieldMap)) {

								switch (field.m_nDataType) {
									case ConstDef.COLLECT_FIELD_DATATYPE_DIGITAL :
										StrNewFieldList += field.m_strFieldName + ",";
										break;
									case ConstDef.COLLECT_FIELD_DATATYPE_STRING :
										StrNewFieldList += field.m_strFieldName + ",";
										// bw.write(field.m_strFieldName);
										break;
									case ConstDef.COLLECT_FIELD_DATATYPE_DATATIME :
										// bw.write(field.m_strFieldName+" Date
										// 'YYYY-MM-DD HH24:MI:SS'");
										StrNewFieldList += field.m_strFieldName + " Date 'YYYY-MM-DD HH24:MI:SS',";
										break;
								}
								break;
							}
						}

					}
					if (StrNewFieldList.length() >= 1)
						StrNewFieldList = StrNewFieldList.substring(0, StrNewFieldList.length() - 1);
					bw.write(StrNewFieldList);
				} else {
					if (isCreateCtlByRawName(tableInfo.fields)) {
						StringBuilder tmp = new StringBuilder();
						tmp.append("OMCID,COLLECTTIME Date 'YYYY-MM-DD HH24:MI:SS',STAMPTIME Date 'YYYY-MM-DD HH24:MI:SS',");
						int i = -1;
						List<Integer> nullList = new ArrayList<Integer>();
						for (String rawName : raws) {
							boolean flag = false;
							i++;
							if (colnameToIndex.containsKey(rawName)) {
								List<Integer> index = colnameToIndex.get(rawName);
								for (int ix : index) {
									if (i == ix) {
										flag = true;
										break;
									}
								}
							}
							if (flag) {
								continue;
							}

							FieldTemplet field = findFieldTemplet(tableInfo.fields, rawName);
							if (field == null) {
								if (i > 2) {
									nullList.add(i);
								}
								continue;
							}
							switch (field.m_nDataType) {
								case ConstDef.COLLECT_FIELD_DATATYPE_DIGITAL :
									tmp.append(field.m_strFieldName);
									break;
								case ConstDef.COLLECT_FIELD_DATATYPE_STRING :
									tmp.append(field.m_strFieldName + " CHAR(" + field.m_strDataTimeFormat + ")");
									break;
								case ConstDef.COLLECT_FIELD_DATATYPE_DATATIME :
									if (field.m_strFieldName.equals("COLLECTTIME")) {
										tmp.append("COLLECT_TIME Date 'YYYY-MM-DD HH24:MI:SS'");
									} else {
										tmp.append(field.m_strFieldName + " Date 'YYYY-MM-DD HH24:MI:SS'");
									}
									break;

								case ConstDef.COLLECT_FIELD_DATATYPE_LOB :
									tmp.append(field.m_strFieldName + " LOBFILE(LOBF_00006) TERMINATED BY EOF ");
									break;
							}

							if (field.m_bIsDefault) {
								bw.write(" " + field.m_strDefaultValue);
							}
							tmp.append(",");
						}

						// chensj 2010-07-16 去掉txt文件中，分发模板未配置的列
						if (nullList.size() > 0) {
							String txtName = currentPath + File.separatorChar + tempFile + ".txt";
							File txt = new File(txtName);
							InputStream is = new FileInputStream(txt);
							BufferedReader reader = new BufferedReader(new InputStreamReader(is));
							String tmpFile = txtName + ".tmp";
							PrintWriter pw = new PrintWriter(tmpFile);
							String str = null;
							while ((str = reader.readLine()) != null) {
								pw.println(str);
								pw.flush();
							}
							pw.close();
							is.close();
							reader.close();
							txt.delete();

							is = new FileInputStream(tmpFile);
							reader = new BufferedReader(new InputStreamReader(is));
							pw = new PrintWriter(txtName);
							while ((str = reader.readLine()) != null) {
								String[] items = str.split(splitSign);
								StringBuilder sb = new StringBuilder();
								for (int j = 0; j < items.length; j++) {
									boolean flag = false;
									for (Integer index : nullList) {
										if (j == index) {
											flag = true;
										}
									}
									if (!flag) {
										sb.append(items[j]).append(splitSign);
									}
								}
								sb.deleteCharAt(sb.length() - 1);
								pw.println(sb);
								pw.flush();
							}
							pw.close();
							is.close();
							reader.close();
							new File(tmpFile).delete();
						}

						if (tmp.charAt(tmp.length() - 1) == ',') {
							tmp.deleteCharAt(tmp.length() - 1);
						}
						bw.write(tmp.toString());
					} else {
						for (int i = 0; i < tableInfo.fields.size(); i++) {
							DistributeTemplet.FieldTemplet field = tableInfo.fields.get(i);
							switch (field.m_nDataType) {
								case ConstDef.COLLECT_FIELD_DATATYPE_DIGITAL :
									bw.write(field.m_strFieldName);
									break;
								case ConstDef.COLLECT_FIELD_DATATYPE_STRING :
									bw.write(field.m_strFieldName + " CHAR(" + field.m_strDataTimeFormat + ")");
									break;
								case ConstDef.COLLECT_FIELD_DATATYPE_DATATIME :
									bw.write(field.m_strFieldName + " Date 'YYYY-MM-DD HH24:MI:SS'");
									break;

								case ConstDef.COLLECT_FIELD_DATATYPE_LOB :
									bw.write(field.m_strFieldName + " LOBFILE(LOBF_00006) TERMINATED BY EOF ");
									break;
							}
							// By Xumg
							// 默认值，为了避免触发器影响效率，用这种方式实现sequence的自增
							// 其它情况的默认值未测试
							if (field.m_bIsDefault) {
								bw.write(" " + field.m_strDefaultValue);
							}
							if (i < tableInfo.fields.size() - 1)
								bw.write(",");
						}
					}
				}

				bw.write(")\r\n");
				bw.flush();
				// 运行Sqlldr

				Date now = new Date();
				collectInfo.setSqlldrTime(new Timestamp(now.getTime()));

				retCode = RunSqlldr(tableIndex, tempFile);
			} else if (Util.isSybase() || Util.isSqlServer()) {
				Map<Integer, String> columns = CommonDB.GetTableColumns(tableInfo.tableName);
				bw.write("10.0\r\n"); // BCP版本
				int nField = tableInfo.fields.size();
				bw.write(String.valueOf(nField) + "\r\n"); // 字段个数

				for (int i = 1; i <= nField; ++i) {
					bw.write(i + "\tSYBCHAR\t0\t128\t");
					if (i < nField) {
						bw.write("\";\"");
					} else {
						bw.write("\"\n\"");
					}

					String strField = tableInfo.fields.get(i - 1).m_strFieldName;
					int j = 0;
					for (; j < columns.size(); ++j) {
						if (strField.equalsIgnoreCase(columns.get(j))) {
							break;
						}
					}
					bw.write("\t" + (j + 1) + "\t" + strField + "\r\n");
				}

				bw.flush();

				java.util.Date now = new java.util.Date();
				collectInfo.setSqlldrTime(new Timestamp(now.getTime()));

				// 运行Bcp
				RunBcp(tableInfo.tableName, tempFile);
			}
			// By xumg end
			// 是否删除日志
			if (SystemConfig.getInstance().isDeleteLog()) {
				// 删除.CTL
				File ctlfile = new File(currentPath, tempFile + ".ctl");
				if (ctlfile.exists())
					ctlfile.delete();

				// 删除.txt文件
				String strTxt = currentPath + File.separatorChar + tempFile + ".txt";
				File txtfile = new File(strTxt);
				if (txtfile.exists()) {
					if (txtfile.delete()) {
						logStr = collectInfo.getSysName() + ": " + strTxt + "删除成功....";
						log.debug(logStr);
						collectInfo.log(DataLogInfo.STATUS_DIST, logStr);
					} else {
						logStr = collectInfo.getSysName() + ": " + strTxt + "删除失败";
						log.debug(logStr);
						collectInfo.log(DataLogInfo.STATUS_DIST, logStr);
					}
				} else {
					logStr = collectInfo.getSysName() + ": " + strTxt + "未找到，无法删除";
					log.debug(logStr);
					collectInfo.log(DataLogInfo.STATUS_DIST, logStr);
				}

				if (retCode == 0) {
					// 删除日志文件
					File txtlog = new File(currentPath + File.separatorChar + "ldrlog", tempFile + ".log");
					if (txtlog.exists())
						txtlog.delete();
				}
			}
		} catch (Exception e) {
			log.error("BuildSqlLdr", e);
			collectInfo.log(DataLogInfo.STATUS_DIST, "BuildSqlLdr", e);
		} finally{
			IOUtils.closeQuietly(bw);
		}
	}

	/*
	 * add by chensj(20100507) 判断是否以rawname的方式来创建ctl文件， 一个<DATATABLE>中，如果所有的<RAWNAME >都有值，那么就以rawname方式创建ctl
	 */
	private boolean isCreateCtlByRawName(Map<Integer, FieldTemplet> fields) {
		Collection<FieldTemplet> fs = fields.values();
		for (FieldTemplet f : fs) {
			if (Util.isNull(f.rawName) && !f.m_strFieldName.equalsIgnoreCase("omcid") && !f.m_strFieldName.equalsIgnoreCase("collecttime")
					&& !f.m_strFieldName.equalsIgnoreCase("stamptime")) {
				return false;
			}
		}
		return true;
	}

	private FieldTemplet findFieldTemplet(Map<Integer, FieldTemplet> fields, String rawName) {
		Map<FieldTemplet, Float> tmp = new HashMap<FieldTemplet, Float>();
		Collection<FieldTemplet> fs = fields.values();
		for (FieldTemplet f : fs) {
			float dis = LevenshteinDistance.similarity(rawName, f.rawName);
			if (dis >= SystemConfig.getInstance().getFieldMatch()) {
				tmp.put(f, dis);
			}
		}

		Iterator<Entry<FieldTemplet, Float>> it = tmp.entrySet().iterator();
		Entry<FieldTemplet, Float> max = null;
		while (it.hasNext()) {
			Entry<FieldTemplet, Float> et = it.next();
			if (max == null || et.getValue() > max.getValue()) {
				max = et;
			}
		}

		return max == null ? null : max.getKey();
	}

	public String ReadFileFirstLine(String TempFile) {
		String strCurrentPath = SystemConfig.getInstance().getCurrentPath();
		FileReader reader;
		String strLine = "";
		try {
			reader = new FileReader(strCurrentPath + "/" + TempFile + ".txt");
			BufferedReader br = new BufferedReader(reader);
			strLine = br.readLine();
			br.close();
			reader.close();
		} catch (Exception e) {
			log.error(collectInfo.getSysName() + " : ReadFileFirstLine", e);
		}

		return strLine;
	}

	public void RunSqlldr(String strCtlName) {
		this.RunSqlldr(0, strCtlName);
	}

	@SuppressWarnings("static-access")
	public int RunSqlldr(int tableIndex, String strCtlName) {
		String logStr = null;
		String strOracleBase = SystemConfig.getInstance().getDbService();
		String strOracleUserName = SystemConfig.getInstance().getDbUserName();
		String strOraclePassword = SystemConfig.getInstance().getDbPassword();
		String strCurrentPath = SystemConfig.getInstance().getCurrentPath();
		int retCode = -1;
		Timestamp sqlldrStartTime = new Timestamp(System.currentTimeMillis());
		String cmd = String.format("sqlldr userid=%s/%s@%s skip=1 control=%s%s%s.ctl bad=%s%s%s.bad log=%s%sldrlog%s%s.log errors=999999",
				strOracleUserName, strOraclePassword, strOracleBase, strCurrentPath, File.separatorChar, strCtlName, strCurrentPath,
				File.separatorChar, strCtlName, strCurrentPath, File.separatorChar, File.separatorChar, strCtlName);
		try {
			BalySqlloadThread sqlthread = new BalySqlloadThread();
			sqlthread.setM_TaskInfo(this.collectInfo);
			sqlthread.setTableIndex(tableIndex);

			logStr = collectInfo.getSysName() + ": " + cmd.replace(strOracleUserName, "*").replace(strOraclePassword, "*");
			log.debug(logStr);
			collectInfo.log(DataLogInfo.STATUS_DIST, logStr);
			// retCode=2 时 为部分成功，也视为成功对待，不进行重试
			retCode = sqlthread.runcmd(cmd);
			if (retCode == 0 || retCode == 2) {
				logStr = collectInfo.getSysName() + ": sqldr OK. retCode=" + retCode;
				log.debug(logStr);
				collectInfo.log(DataLogInfo.STATUS_DIST, logStr);
			} else if (retCode != 0 && retCode != 2) {
				int maxTryTimes = 3;
				int tryTimes = 0;
				long waitTimeout = 30 * 1000;
				while (tryTimes < maxTryTimes) {
					retCode = sqlthread.runcmd(cmd);
					if (retCode == 0 || retCode == 2) {
						break;
					}

					tryTimes++;
					waitTimeout = 2 * waitTimeout;

					logStr = collectInfo.getSysName() + ": 第" + tryTimes + "次Sqlldr尝试入库失败. " + cmd + " retCode=" + retCode;
					log.error(logStr);
					collectInfo.log(DataLogInfo.STATUS_DIST, logStr);

					Thread.currentThread().sleep(waitTimeout);
				}

				// 如果重试超过 maxTryTimes 次还失败则记录日志
				if (retCode == 0 || retCode == 2) {
					logStr = collectInfo.getSysName() + ": " + tryTimes + "次Sqlldr尝试入库后成功. retCode=" + retCode;
					log.info(logStr);
					collectInfo.log(DataLogInfo.STATUS_DIST, logStr);
				} else {
					logStr = collectInfo.getSysName() + " : " + tryTimes + "次Sqlldr尝试入库失败. " + cmd + " retCode=" + retCode;
					log.error(logStr);
					collectInfo.log(DataLogInfo.STATUS_DIST, logStr);
					// 通知告警
					AlarmMgr.getInstance().insert(collectInfo.getTaskID(), "sqlldr 失败 重试" + tryTimes + "次",
							collectInfo.getSysName() + " 返回值=" + retCode, cmd, 1504);
				}
			} else {
				logStr = collectInfo.getSysName() + ": sqlldr 失败 并且不重试.";
				log.error(logStr);
				collectInfo.log(DataLogInfo.STATUS_DIST, logStr);

				// 通知告警
				AlarmMgr.getInstance().insert(collectInfo.getTaskID(), "sqlldr 失败 并且不重试", collectInfo.getSysName() + " 返回值=" + retCode, cmd, 1503);
			}
		} catch (Exception e) {
			logStr = collectInfo.getSysName() + ": sqlldr exception. " + cmd;
			log.error(logStr, e);
			collectInfo.log(DataLogInfo.STATUS_DIST, logStr, e);

			// 通知告警
			AlarmMgr.getInstance().insert(collectInfo.getTaskID(), "sqlldr 异常", collectInfo.getSysName(), cmd + e.getMessage(), 1506);
		}

		String logFileName = strCurrentPath + File.separator + "ldrlog" + File.separator + strCtlName + ".log";
		File logFile = new File(logFileName);
		if (!logFile.exists() || !logFile.isFile()) {
			logStr = collectInfo.getSysName() + ": " + logFileName + "不存在.";
			log.info(logStr);
			collectInfo.log(DataLogInfo.STATUS_DIST, logStr);
			return retCode;
		}
		SqlLdrLogAnalyzer analyzer = new SqlLdrLogAnalyzer();
		FileInputStream fs = null;
		try {
			fs = new FileInputStream(logFileName);
			SqlldrResult result = analyzer.analysis(fs);
			if (result == null)
				return retCode;

			logStr = collectInfo.getSysName() + ": SQLLDR日志分析结果: omcid=" + collectInfo.getDevInfo().getOmcID() + " 表名=" + result.getTableName()
					+ " 数据时间=" + Util.getDateString(collectInfo.getLastCollectTime()) + " 入库成功条数=" + result.getLoadSuccCount() + " sqlldr日志="
					+ logFileName;
			log.debug(logStr);
			collectInfo.log(DataLogInfo.STATUS_DIST, logStr);

			dbLogger.log(collectInfo.getDevInfo().getOmcID(), result.getTableName(), collectInfo.getLastCollectTime(), result.getLoadSuccCount(),
					collectInfo.getTaskID(), collectInfo.getGroupId(), result.getLoadSuccCount() + result.getLoadFailCount(), sqlldrStartTime,
					retCode);
		} catch (Exception e) {
			logStr = collectInfo.getSysName() + ": sqlldr日志分析失败，文件名：" + logFileName + "，原因: ";
			log.error(logStr, e);
			collectInfo.log(DataLogInfo.STATUS_DIST, logStr, e);
		} finally{
			IOUtils.closeQuietly(fs);
		}
		return retCode;
	}

	private void RunBcp(String strTable, String strFormat) {
		Process ldr = null;
		try {
			String strCurrentPath = SystemConfig.getInstance().getCurrentPath();
			String strUrl = SystemConfig.getInstance().getDbUrl();
			String strBase = strUrl.substring(strUrl.lastIndexOf("/") + 1);
			String strUserName = SystemConfig.getInstance().getDbUserName();
			String strPassword = SystemConfig.getInstance().getDbPassword();
			String strService = SystemConfig.getInstance().getDbService();
			String strLog = strCurrentPath + File.separatorChar + "ldrlog" + File.separatorChar + strFormat + ".log";
			String strDataFile = strCurrentPath + File.separatorChar + strFormat + ".txt";

			String cmd = String.format("bcp %s..%s in \"%s\" -U%s -P%s -S%s -t; -r\\n -c -e %s", strBase, strTable, strDataFile, strUserName,
					strPassword, strService, strLog);

			ldr = Runtime.getRuntime().exec(cmd);
			ldr.waitFor();
		} catch (Exception e) {
			log.error("BCP Error!", e);
		} finally{
			if(ldr != null){
				ldr.destroy();
			}
		}
	}

}
