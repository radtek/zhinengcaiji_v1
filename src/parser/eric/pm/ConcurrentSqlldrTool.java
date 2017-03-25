package parser.eric.pm;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Map.Entry;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;

import parser.eric.pm.DBFunction.CalCfgItem;
import task.CollectObjInfo;
import util.DBLogger;
import util.ExternalCmd;
import util.LogMgr;
import util.SqlldrResult;
import util.Util;
import util.loganalyzer.SqlLdrLogAnalyzer;
import framework.SystemConfig;

/**
 * 并发SQLLDR入库工具。用于W网爱立信性能入库，以一个时间点的任务为单位入库。即一个时间点的任务，把所有文件都解析写入TXT文件后，入库， 而不是每个文件写一次SQLLDR文件入库。
 * 
 * @author ChenSijiang
 */
public class ConcurrentSqlldrTool {

	private CollectObjInfo task;

	private String logKey;

	private SqlLdrLogAnalyzer an = new SqlLdrLogAnalyzer();

	// 表名 - SQLLDR信息
	private Map<String, SqlldrInfo> infos = new HashMap<String, SqlldrInfo>();

	// txt文件全路径 - 记录数
	private Map<String, Integer> counts = new HashMap<String, Integer>();

	private static final Logger logger = LogMgr.getInstance().getSystemLogger();

	private static final Random RND = new Random();

	private static final DBLogger dbLogger = LogMgr.getInstance().getDBLogger();

	private static final SystemConfig cfg = SystemConfig.getInstance();

	private static final String LINE_SEPARATOR = System.getProperty("line.separator");

	public ConcurrentSqlldrTool(CollectObjInfo task) {
		super();
		this.task = task;
		this.logKey = String.format("[%s][%s]", task.getTaskID(), Util.getDateString(task.getLastCollectTime()));
	}

	Map<String, List<CalCfgItem>> calCols = new HashMap<String, List<CalCfgItem>>();

	public synchronized void writeTable(SqlldrParam param) {
		String tn = param.tbName.toUpperCase();

		SqlldrInfo info = null;
		if (!infos.containsKey(tn)) {
			File dir = new File(SystemConfig.getInstance().getCurrentPath() + File.separator + "ldrlog" + File.separator + "eric_w_pm"
					+ File.separator + task.getTaskID() + File.separator);
			dir.mkdirs();
			String strDateTime = Util.getDateString_yyyyMMddHHmmss(task.getLastCollectTime());
			int rnum = RND.nextInt(Integer.MAX_VALUE);
			String name = task.getTaskID() + "_" + tn + "_" + strDateTime + "_" + rnum + "_" + System.nanoTime();
			info = new SqlldrInfo(new File(dir, name + ".txt"), new File(dir, name + ".log"), new File(dir, name + ".bad"), new File(dir, name
					+ ".ctl"));
			infos.put(tn, info);

			// 写txt表头和ctl文件
			String columnList = param.listColumnWithLargeColumn(",");
			info.writerForCtl.println("load data");
			info.writerForCtl.println("CHARACTERSET " + cfg.getSqlldrCharset());
			info.writerForCtl.println("infile '" + info.txt.getAbsolutePath() + "' append into table " + tn);
			info.writerForCtl.println("FIELDS TERMINATED BY \";\"");
			info.writerForCtl.println("TRAILING NULLCOLS");
			info.writerForCtl.print("(");
			List<CalCfgItem> ccs = null;
			if (!calCols.containsKey(tn)) {
				ccs = DBFunction.findNeedCal(tn);
				calCols.put(tn, ccs);
			} else
				ccs = calCols.get(tn);
			StringBuilder tmpc = new StringBuilder();
			for (CalCfgItem cc : ccs) {
				tmpc.append(cc.getAdditionColMax()).append(",");
				tmpc.append(cc.getAdditionColAvg()).append(",");
			}
			info.writerForCtl.print(tmpc);
			info.writerForCtl.print(columnList + ",");
			info.writerForCtl.print("OMCID,COLLECTTIME Date 'YYYY-MM-DD HH24:MI:SS'," + "STAMPTIME Date 'YYYY-MM-DD HH24:MI:SS'");
			info.writerForCtl.print(")");
			info.writerForCtl.flush();
			info.writerForCtl.close();
			info.writerForTxt.write(tmpc.toString().replace(",", ";"));
			info.writerForTxt.write(param.listColumn(";") + ";OMCID;COLLECTTIME;STAMPTIME");
			info.writerForTxt.write(LINE_SEPARATOR);
			info.writerForTxt.flush();

		} else
			info = infos.get(tn);

		// 写数据
		String nowStr = Util.getDateString(new Date());
		String sysFieldValue = param.omcID + ";" + nowStr + ";" + Util.getDateString(param.dataTime);

		String commonFieldValue = param.fieldValue2String(param.commonFields, ";"); // 获取公共字段值列表

		Collection<ArrayList<String>> records = param.records.values(); // 取出此表所有数据

		// 处理字段顺序，因为每个文件中的字段顺序可能不同，而控制文件中的字段顺序是以第一个文件为准的。
		String firstLine = null;
		Map<String, Integer> currColIndex = new HashMap<String, Integer>();// 当前数据的列索引位置
		try {
			InputStream inp = new FileInputStream(info.txt);
			List<String> list = IOUtils.readLines(inp);
			firstLine = list.get(0);
			inp.close();
			list.clear();
			list = null;
			String columnList = param.listColumnWithLargeColumn(",");
			String[] item = columnList.split(",");
			for (int i = 0; i < item.length - 4; i++)
				currColIndex.put(item[i], i);
		} catch (Exception e) {
			logger.error(logKey + "处理字段出错", e);
		}
		if (firstLine != null) {
			Map<String, Integer> realColIndex = new HashMap<String, Integer>(); // 存放每个字段的索引位置（已经写在CTL/TXT文件中的）
			String[] items = firstLine.split(";");
			for (int i = 0; i < items.length - 7; i++)
				realColIndex.put(items[i], i);
			for (List<String> record : records) {

				List<String> tmp = new ArrayList<String>(record);
				record.clear();
				for (int i = 0; i < realColIndex.size(); i++)
					record.add(null);
				for (String s : currColIndex.keySet()) {
					if (realColIndex.containsKey(s)) {
						try {
							int index = realColIndex.get(s);
							String val = tmp.get(currColIndex.get(s));
							record.set(index, val);
						} catch (Exception e) {
						}
					}
				}

				if (calCols.containsKey(tn)) {
					List<CalCfgItem> ccs = calCols.get(tn);
					int index = 0;
					for (int i = 0; i < ccs.size(); i++) {
						try {
							Integer colNameIndex = currColIndex.get(ccs.get(i).getColName());
							String vv = null;
							if (colNameIndex != null)
								vv = tmp.get(colNameIndex);

							if (vv != null) {
								Double m = DBFunction.getMaxAvgCounter(vv, 1, ccs.get(i).getCounterGroup(), ccs.get(i));
								Double a = DBFunction.getMaxAvgCounter(vv, 2, ccs.get(i).getCounterGroup(), ccs.get(i));
								String strM = (m != null ? String.format("%.5f", m) : "");
								String strA = (a != null ? String.format("%.5f", a) : "");
								record.set(index++, strM);
								record.set(index++, strA);
							} else {
								record.set(index++, "");
								record.set(index++, "");
							}

						} catch (Exception e) {
							logger.error(
									logKey + "  tn  - " + tn + "   col - " + ccs.get(i).getColName() + "  map - " + currColIndex + "  "
											+ e.getMessage(), e);
							if (tmp == null) {
								logger.warn(logKey + "tmp null");
							}
							if (currColIndex == null) {
								logger.warn(logKey + "currColIndex null");
							}
							if (ccs == null) {
								logger.warn(logKey + "ccs null");
							}
							if (ccs.get(i) == null) {
								logger.warn(logKey + "ccs.get(i) null");
							}
							if (ccs.get(i).getColName() == null) {
								logger.warn(logKey + "ccs.get(i).getColName()  null");
							}
							if (currColIndex.get(ccs.get(i).getColName()) == null) {
								logger.warn(logKey + " currColIndex.get(ccs.get(i).getColName())  null");
							}
						}

					}
				}
			}
		}
		for (ArrayList<String> record : records) {
			info.writerForTxt.write(list2Str(record, ";") + ";" + commonFieldValue + ";" + sysFieldValue);
			info.writerForTxt.write(LINE_SEPARATOR);
		}

		info.writerForTxt.flush();

		// 统计记录数
		String txtpath = info.txt.getAbsolutePath();
		if (counts.containsKey(txtpath))
			counts.put(txtpath, counts.get(txtpath) + param.records.size());
		else
			counts.put(txtpath, param.records.size());
	}

	public void runSqlldr() {

		// 写fd文件，给s3用。
		if (Util.isNotNull(SystemConfig.getInstance().getFdPath())) {
			Iterator<Entry<String, Integer>> eCounts = counts.entrySet().iterator();
			Date date = new Date();

			File dir = new File(SystemConfig.getInstance().getFdPath() + File.separator);
			if (!dir.exists())
				dir.mkdirs();

			File fdPath = new File(SystemConfig.getInstance().getFdPath() + File.separator + Util.getDateString_yyyyMMddHHmmssSSS(date) + "_"
					+ task.getTaskID() + ".fd");
			Document fd = DocumentHelper.createDocument();
			Element root = fd.addElement("ds_desc");
			root.addElement("group").addAttribute("id", String.valueOf(task.getGroupId()));
			root.addElement("task").addAttribute("id", String.valueOf(task.getTaskID()));
			root.addElement("stamptime").addAttribute("value", Util.getDateString(date));
			root.addElement("omc").addAttribute("id", String.valueOf(task.getDevInfo().getOmcID()));
			root.addElement("datatime").addAttribute("value", Util.getDateString(task.getLastCollectTime()));
			Element files = root.addElement("files");
			while (eCounts.hasNext()) {
				Entry<String, Integer> et = eCounts.next();
				files.addElement("file").addAttribute("url", et.getKey()).addAttribute("count", String.valueOf(et.getValue()));
			}
			OutputFormat fmt = OutputFormat.createPrettyPrint();
			fmt.setEncoding("utf-8");
			try {
				OutputStream fdOut = new FileOutputStream(fdPath);
				XMLWriter xw = new XMLWriter(fdOut, fmt);
				xw.write(fd);
				xw.flush();
				xw.close();
				fdOut.close();
			} catch (Exception e) {
				logger.error("写FD文件时异常", e);
			}
		}

		Iterator<String> it = infos.keySet().iterator();
		while (it.hasNext()) {
			String tableName = it.next();
			SqlldrInfo info = infos.get(tableName);
			info.close();
			if (tableName.equalsIgnoreCase("CLT_PM_W_ERIC_HSDSCHRESOURCES")) {
				CQIHandler.handCQI(info.txt, info.ctl);
			}
			String cmd = String.format("sqlldr userid=%s/%s@%s skip=%s control=%s bad=%s log=%s errors=9999999", cfg.getDbUserName(),
					cfg.getDbPassword(), cfg.getDbService(), 1, info.ctl.getAbsoluteFile(), info.bad.getAbsoluteFile(), info.log.getAbsoluteFile());
			int ret = -1;
			try {
				logger.debug(logKey + "执行sqlldr - " + cmd.replace(cfg.getDbPassword(), "*"));
				ret = new ExternalCmd().execute(cmd);
				SqlldrResult result = an.analysis(info.log.getAbsolutePath());
				dbLogger.log(this.task.getDevInfo().getOmcID(), result.getTableName(), this.task.getLastCollectTime(), result.getLoadSuccCount(),
						this.task.getTaskID());
				logger.debug(logKey
						+ String.format("ret=%s,入库条数=%s,task_id=%s,表名=%s,时间点=%s", ret, result.getLoadSuccCount(), this.task.getTaskID(),
								result.getTableName(), Util.getDateString(this.task.getLastCollectTime())));
				if (ret == 0 && cfg.isDeleteLog()) {
					// txt文件不删，交给s3管理。
					if (Util.isNull(SystemConfig.getInstance().getFdPath()))
						info.txt.delete();
					info.log.delete();
					info.ctl.delete();
					info.bad.delete();
				}
			} catch (Exception e) {
				logger.error(logKey + "sqlldr时异常", e);
			}
		}
		infos.clear();
	}

	private String list2Str(List<String> lst, String splitSign) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < lst.size() - 1; i++) {
			String value = lst.get(i);
			value = value == null ? "" : value;
			sb.append(value).append(splitSign);
		}
		String lastElement = lst.get(lst.size() - 1);
		lastElement = lastElement == null ? "" : lastElement;
		sb.append(lastElement);

		return sb.toString();
	}

	private class SqlldrInfo implements Closeable {

		File txt;

		File log;

		File bad;

		File ctl;

		PrintWriter writerForTxt;

		PrintWriter writerForCtl;

		public SqlldrInfo(File txt, File log, File bad, File ctl) {
			super();
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

	}
}
