package parser.hw.mml;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import task.CollectObjInfo;
import task.DevInfo;
import util.CommonDB;
import util.DbPool;
import util.ExternalCmd;
import util.LogMgr;
import util.SqlldrResult;
import util.Util;
import util.loganalyzer.SqlLdrLogAnalyzer;
import framework.SystemConfig;

/**
 * 解析mml脚本，并以sqlldr方式入库
 * 
 * @author ChenSijiang 2011-4-7 下午06:42:08
 */
public class MML_Sqlldr {

	CollectObjInfo task; // 任务信息

	String file; // 原始文件路径

	String bscid;

	String logKey;

	private String stamptime;

	private String omcid;

	private String collecttime;

	private Timestamp tsStamptime;

	private Timestamp tsCollecttime;

	Map<String/* 表名 */, List<String>/* 表的所有列 */> tableCols = new HashMap<String, List<String>>();

	private static final Map<String/* 表名 */, List<String>/* 表的所有列 */> TABLE_COLS = new HashMap<String, List<String>>();

	private static final String CLT_PREFIX = "CLT_CM_W_HW_";

	private static final Logger logger = LogMgr.getInstance().getSystemLogger();

	private static Map<String/* tableName */, Map<String/* columnName */, String/* type */>> specialTableMap = null;

	static {
		specialTableMap = new HashMap<String, Map<String, String>>();

		Map<String, String> specialColumnMap = new HashMap<String, String>();
		specialColumnMap.put("NBMCACALGOSWITCH", "char(10000)");
		String tableName1 = "CLT_CM_W_HW_CELLALGOSWITCH";
		specialTableMap.put(tableName1, specialColumnMap);
	}

	/**
	 * U开头的表名
	 */
	private static final List<String> SPEC = new ArrayList<String>();
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
		Connection con = null;
		PreparedStatement st = null;
		ResultSet rs = null;
		try {
			con = DbPool.getConn();
			String sql = "select table_name,column_name from user_tab_cols where TABLE_NAME like ?";
			st = con.prepareStatement(sql);
			st.setString(1, "CLT_CM_W_HW_%");
			rs = st.executeQuery();
			while (rs.next()) {
				String tn = rs.getString("table_name");
				if (tn.contains("_X_"))
					continue;
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
			logger.error("载入华为W参数表结构时出错", e);
		} finally {
			CommonDB.closeDBConnection(con, st, rs);
		}
		TABLE_COLS.remove("CLT_CM_W_HW_MAP");
	}

	public MML_Sqlldr(CollectObjInfo task, String file) {
		super();
		this.task = task;
		this.file = file;
		this.stamptime = Util.getDateString(task.getLastCollectTime());
		this.tsCollecttime = new Timestamp(System.currentTimeMillis());
		this.tsStamptime = new Timestamp(task.getLastCollectTime().getTime());
		this.collecttime = Util.getDateString(this.tsCollecttime);
		this.omcid = String.valueOf(task.getDevInfo().getOmcID());
		this.logKey = String.format("[%s][%s]", task.getTaskID(), this.stamptime);
	}

	public boolean parse() {
		File mmlFile = new File(this.file);
		logger.info(logKey + "开始解析 - " + mmlFile.getAbsolutePath());
		InputStream in = null;
		InputStreamReader isr = null;
		BufferedReader br = null;
		try {
			Map<String, SqlldrFiles> sqlldrs = buildSqlldr();

			in = new FileInputStream(mmlFile);
			isr = new InputStreamReader(in);
			br = new BufferedReader(isr);

			String line = null;
			while ((line = br.readLine()) != null) {
				String trimedLine = line.trim();
				if (isDataLine(trimedLine)) {
					// 处理ADD,MOD,SET开头的数据行。
					LineData lineData = LineData.getLineData(trimedLine, bscid, omcid, stamptime, collecttime);
					if (lineData != null) {
						List<String> cols = TABLE_COLS.get(lineData.getTableName());
						if (cols != null) {
							StringBuilder txtLine = new StringBuilder();// 要写到sqlldr数据文件中的一行
							for (int i = 0; i < cols.size(); i++) {
								String val = lineData.getDatas().get(cols.get(i));
								txtLine.append(Util.isNull(val) ? "" : val).append(";");
							}
							txtLine.delete(txtLine.length() - 1, txtLine.length());
							sqlldrs.get(lineData.getTableName()).writerForTxt.println(txtLine);
							sqlldrs.get(lineData.getTableName()).writerForTxt.flush();
							txtLine.setLength(0);
							txtLine = null;
							lineData = null;
						} else {
							// logger.warn(logKey + "未找到对应采集表，原始数据行:" + line);
						}
					}
				} else if (trimedLine.startsWith("//System") && this.bscid == null) {
					// 在文件注释行中找出bscid
					try {
						this.bscid = trimedLine.substring(trimedLine.indexOf(":") + 1).trim();
					} catch (Exception e) {
						logger.error(logKey + "试图获取bscid时发生异常，当前数据行:" + line, e);
					}
				}
			}
			logger.info(logKey + "解析完成 - " + mmlFile.getAbsolutePath());

			logger.info(logKey + "开始入库 - " + mmlFile.getAbsolutePath());
			store(sqlldrs);
			logger.info(logKey + "入库完成 - " + mmlFile.getAbsolutePath());
		} catch (Exception e) {
			logger.error(logKey + "解析/入库MML文件时异常", e);
			return false;
		} finally {
			IOUtils.closeQuietly(br);
			IOUtils.closeQuietly(isr);
			IOUtils.closeQuietly(in);
		}
		return true;
	}

	private boolean isDataLine(String trimedLine) {
		return trimedLine.startsWith("ADD ") || trimedLine.startsWith("MOD ") || trimedLine.startsWith("SET ");
	}

	private void store(Map<String, SqlldrFiles> sqlldrInfos) {
		Iterator<Entry<String, SqlldrFiles>> it = sqlldrInfos.entrySet().iterator();
		while (it.hasNext()) {
			Entry<String, SqlldrFiles> e = it.next();
			String cmd = String.format("sqlldr userid=%s/%s@%s skip=1 control=%s bad=%s log=%s errors=9999999", SystemConfig.getInstance()
					.getDbUserName(), SystemConfig.getInstance().getDbPassword(), SystemConfig.getInstance().getDbService(), e.getValue().ctl
					.getAbsoluteFile(), e.getValue().bad.getAbsoluteFile(), e.getValue().log.getAbsoluteFile());
			e.getValue().close();
			logger.debug(logKey + "执行 "
					+ cmd.replace(SystemConfig.getInstance().getDbPassword(), "*").replace(SystemConfig.getInstance().getDbUserName(), "*"));
			ExternalCmd execute = new ExternalCmd();
			try {
				int ret = execute.execute(cmd);
				SqlldrResult result = new SqlLdrLogAnalyzer().analysis(e.getValue().log.getAbsolutePath());
				logger.debug(logKey + "exit=" + ret + " omcid=" + omcid + " 入库成功条数=" + result.getLoadSuccCount() + " 表名=" + result.getTableName()
						+ " 数据时间=" + stamptime + " sqlldr日志=" + e.getValue().log.getAbsolutePath());
				LogMgr.getInstance().getDBLogger()
						.log(task.getDevInfo().getOmcID(), result.getTableName(), tsStamptime, result.getLoadSuccCount(), task.getTaskID());
				if (ret == 0 && SystemConfig.getInstance().isDeleteLog()) {
					e.getValue().txt.delete();
					e.getValue().ctl.delete();
					e.getValue().log.delete();
					e.getValue().bad.delete();
				}
			} catch (Exception ex) {
				logger.error(logKey + "sqlldr时异常", ex);
			}
		}

	}

	private Map<String, SqlldrFiles> buildSqlldr() throws Exception {
		Map<String, SqlldrFiles> map = new HashMap<String, SqlldrFiles>();

		File baseDir = new File(SystemConfig.getInstance().getCurrentPath() + File.separator + "ldrlog" + File.separator + "w_cm_hw_mml"
				+ File.separator + task.getTaskID() + File.separator);
		if (!baseDir.exists())
			baseDir.mkdirs();

		long curr = System.currentTimeMillis();

		Iterator<Entry<String, List<String>>> it = TABLE_COLS.entrySet().iterator();
		while (it.hasNext()) {
			Entry<String, List<String>> e = it.next();
			String tb = e.getKey();
			List<String> cols = e.getValue();

			String baseFilename = baseDir.getAbsolutePath() + File.separator + task.getTaskID() + "_"
					+ Util.getDateString_yyyyMMddHH(task.getLastCollectTime()) + "_" + tb.toUpperCase() + "_" + curr;
			File txt = new File(baseFilename + ".txt");
			File log = new File(baseFilename + ".log");
			File bad = new File(baseFilename + ".bad");
			File ctl = new File(baseFilename + ".ctl");
			SqlldrFiles info = new SqlldrFiles(txt, log, bad, ctl);
			map.put(tb.toUpperCase(), info);

			info.writerForCtl.println("load data");
			info.writerForCtl.println("CHARACTERSET " + SystemConfig.getInstance().getSqlldrCharset());
			info.writerForCtl.println("INFILE '" + txt.getAbsolutePath() + "' APPEND INTO TABLE " + tb.toUpperCase());
			info.writerForCtl.println("FIELDS TERMINATED BY \";\"");
			info.writerForCtl.println("TRAILING NULLCOLS");
			info.writerForCtl.println("(");
			for (int i = 0; i < cols.size(); i++) {
				String col = cols.get(i).toUpperCase().trim();
				info.writerForTxt.print(col);

				if (col.equals("COLLECTTIME") || col.equals("STAMPTIME")) {
					info.writerForCtl.print(col + " DATE 'YYYY-MM-DD HH24:MI:SS'");
				} else {
					info.writerForCtl.print(col);
					if (specialTableMap.get(tb.toUpperCase()) != null && specialTableMap.get(tb.toUpperCase()).get(col) != null) {
						info.writerForCtl.print(" " + specialTableMap.get(tb.toUpperCase()).get(col));
					}
				}

				if (i < cols.size() - 1) {
					info.writerForCtl.print(",");
					info.writerForTxt.print(";");
				}
				info.writerForCtl.println();
			}
			info.writerForCtl.print(")");
			info.writerForCtl.flush();
			info.writerForTxt.println();
			info.writerForTxt.flush();

		}

		return map;
	}

	// 封闭原始文件中的一行数据
	private static class LineData {

		private String dataName; // 数据名，比如ADD BRD开头的数据行，数据名是BRD

		private String tableName; // 对应采集表名

		private Map<String/* 列名 */, String/* 值 */> datas = new HashMap<String, String>();// 存储所有列名与值

		private LineData() {
			super();
		}

		public static LineData getLineData(String trimedLine, String bscId, String omcId, String stamptime, String collecttime) {
			LineData ld = new LineData();
			try {
				int bIndex = trimedLine.indexOf(":"); // 冒号的位置
				ld.dataName = trimedLine.substring(trimedLine.indexOf(" "), bIndex).trim();

				if (ld.dataName.startsWith("U"))
					ld.tableName = (SPEC.contains(ld.dataName) ? CLT_PREFIX + ld.dataName : CLT_PREFIX
							+ ld.dataName.substring(1, ld.dataName.length()));
				else
					ld.tableName = CLT_PREFIX + ld.dataName;

				String str = trimedLine.substring(trimedLine.indexOf(":") + 1).trim().replace(";", "");
				String[] splited = str.split(",");
				for (String s : splited) {
					String[] keyAndVal = s.split("=");
					String val = keyAndVal.length > 1 ? keyAndVal[1].trim().replace("\"", "") : "";
					ld.datas.put(keyAndVal[0].trim().replace("\"", ""), val);
				}
			} catch (Exception e) {
				logger.error("分析数据时异常，数据行:" + trimedLine);
				return null;
			}
			ld.datas.put("RNCID_X", bscId);
			ld.datas.put("STAMPTIME", stamptime);
			ld.datas.put("COLLECTTIME", collecttime);
			ld.datas.put("OMCID", omcId);
			return ld;
		}

		public String getTableName() {
			return tableName;
		}

		public Map<String, String> getDatas() {
			return datas;
		}

		@Override
		public String toString() {
			return "LineData [dataName=" + dataName + ", datas=" + datas + ", tableName=" + tableName + "]";
		}

	}

	// 封装sqlldr时要用到的文件（ctl,txt等）
	private class SqlldrFiles implements Closeable {

		File txt;

		File ctl;

		File log;

		File bad;

		PrintWriter writerForTxt;

		PrintWriter writerForCtl;

		public SqlldrFiles(File txt, File log, File bad, File ctl) {
			super();
			this.txt = txt;
			this.log = log;
			this.bad = bad;
			this.ctl = ctl;
			try {
				writerForTxt = new PrintWriter(txt);
				writerForCtl = new PrintWriter(ctl);
			} catch (Exception ex) {
				logger.error(logKey + "创建txt/ctl文件时发生异常", ex);
			}
		}

		@Override
		public void close() {
			IOUtils.closeQuietly(writerForCtl);
			IOUtils.closeQuietly(writerForTxt);
		}
	}

	public static void main(String[] args) throws Exception {
		// LineData ld =
		// LineData.getLineData("ADD U2GNCELL:RNCID=136, CELLID=12411, GSMCELLINDEX=48720, BLINDHOFLAG=FALSE, NPRIOFLAG=FALSE, CIOOFFSET=0, QOFFSET1SN=0, QRXLEVMIN=-50, TPENALTYHCSRESELECT=D0, TEMPOFFSET1=D3, DRDECN0THRESHHOLD=-18, SIB11IND=TRUE, SIB12IND=FALSE, MBDRFLAG=FALSE, MBDRPRIO=0;",
		// "123");
		// System.out.println(ld);
		CollectObjInfo obj = new CollectObjInfo(755123);
		DevInfo dev = new DevInfo();
		dev.setOmcID(101);
		obj.setDevInfo(dev);
		obj.setLastCollectTime(new Timestamp(Util.getDate1("2012-01-04 00:00:00").getTime()));
		MML_Sqlldr pp = new MML_Sqlldr(obj, "F:\\资料\\原始数据\\pk_6\\优网\\CFGMML-RNC768-20120104000510.txt");
		pp.parse();
	}
}
