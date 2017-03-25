package parser.lucent.cm.cdma;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;

import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.Logger;

import task.CollectObjInfo;
import task.DevInfo;
import util.CommonDB;
import util.DBLogger;
import util.DbPool;
import util.ExternalCmd;
import util.LogMgr;
import util.SqlldrResult;
import util.Util;
import util.loganalyzer.SqlLdrLogAnalyzer;
import exception.StoreException;
import framework.SystemConfig;

/**
 * 阿朗DO参数
 * 
 * @author ChenSijiang 2010-11-10
 */
public class CMParser {

	private CollectObjInfo info;

	private String key;

	private String omcId;

	private String stamptime;

	private String collectTime;

	private Map<String, SqlldrInfo> sqlldrInfos = new HashMap<String, SqlldrInfo>();

	private static final Map<String, List<String>> TABLES_COLS = new HashMap<String, List<String>>();

	private static String TABLE_PREFIX = "CLT_CM_LUC_DO_";

	private static final Map<String, String> SHORT_MAP = new HashMap<String, String>();

	private static Logger logger = LogMgr.getInstance().getSystemLogger();

	private DBLogger dbLogger = LogMgr.getInstance().getDBLogger();

	private static final boolean isSPAS = SystemConfig.getInstance().isSPAS();

	static {
		SHORT_MAP.put("uatiRegistrationCompressionTimer", "UATIREGISTRATIONCOMPRESSIONTIM");
		SHORT_MAP.put("uncompressedUatiArraySizeFactor", "UNCOMPRESSEDUATIARRAYSIZEFACTO");
		SHORT_MAP.put("cbcmcsBroadcastOverheadStartTime", "CBCMCSBROADCASTOVERHEADSTARTTI");
		SHORT_MAP.put("bcmcsServiceActiveStandbyStatus", "BCMCSSERVICEACTIVESTANDBYSTATU");
		SHORT_MAP.put("maxStepUpPwrCtrlThshROLNoDatSte", "MAXSTEPUPPWRCTRLTHSHROLNODATST");
		SHORT_MAP.put("stepUpPwrCtrlThshROLNormalState", "STEPUPPWRCTRLTHSHROLNORMALSTAT");
		SHORT_MAP.put("stepUpPwrCtrlThshROLNoDataState", "STEPUPPWRCTRLTHSHROLNODATASTAT");
		SHORT_MAP.put("hybridATTuneawayDetectionEnable", "HYBRIDATTUNEAWAYDETECTIONENABL");
		SHORT_MAP.put("gstepUpPwrCtrlThshROLNormalState", "GSTEPUPPWRCTRLTHSHROLNORMALSTA");
		SHORT_MAP.put("gstepUpPwrCtrlThshROLNoDataState", "GSTEPUPPWRCTRLTHSHROLNODATASTA");
		SHORT_MAP.put("gmaxStepUpPwrCtrlThshROLNoDatSte", "GMAXSTEPUPPWRCTRLTHSHROLNODATS");
		SHORT_MAP.put("hdrOverloadControlForAPProcessor", "HDROVERLOADCONTROLFORAPPROCESS");
		SHORT_MAP.put("enableOHMReliabilityEnhancements", "ENABLEOHMRELIABILITYENHANCEMEN");
		SHORT_MAP.put("multiCarrierCoverageDifferences", "MULTICARRIERCOVERAGEDIFFERENCE");
		SHORT_MAP.put("cpCustomPDSNTierSelectionEnable", "CPCUSTOMPDSNTIERSELECTIONENABL");
		SHORT_MAP.put("oamCustomPDSNTierSelectionEnable", "OAMCUSTOMPDSNTIERSELECTIONENAB");
		SHORT_MAP.put("grestrictRevAATsSupportedCDMACh", "GRESTRICTREVAATSSUPPORTEDCDMAC");
		SHORT_MAP.put("enableImplicitQoSReservationOpen", "ENABLEIMPLICITQOSRESERVATIONOP");
		SHORT_MAP.put("ginServiceSectorCarrierAPersist0", "GINSERVICESECTORCARRIERAPERSI0");
		SHORT_MAP.put("ginServiceSectorCarrierAPersist1", "GINSERVICESECTORCARRIERAPERSI1");
		SHORT_MAP.put("ginServiceSectorCarrierAPersist2", "GINSERVICESECTORCARRIERAPERSI2");
		SHORT_MAP.put("ginServiceSectorCarrierAPersist3", "GINSERVICESECTORCARRIERAPERSI3");
		SHORT_MAP.put("dataOffsetNomRenegotiationEnable", "DATAOFFSETNOMRENEGOTIATIONENAB");
		SHORT_MAP.put("gGmsaMaxNumberOfQoSUsersAdmitted", "GGMSAMAXNUMBEROFQOSUSERSADMITT");
		SHORT_MAP.put("a13SessionTransferCompatibility", "A13SESSIONTRANSFERCOMPATIBILIT");
		SHORT_MAP.put("newForcedDormancyAlgorithmEnable", "NEWFORCEDDORMANCYALGORITHMENAB");
		SHORT_MAP.put("rstrctveAccessAaaSelectionEnable", "RSTRCTVEACCESSAAASELECTIONENAB");
		SHORT_MAP.put("maxDiffChanEntInNbrListSentToAT", "MAXDIFFCHANENTINNBRLISTSENTTOA");
		SHORT_MAP.put("strongestDiffSameChPltPNForIFHO", "STRONGESTDIFFSAMECHPLTPNFORIFH");
		SHORT_MAP.put("inServiceSectorCarrierAPersist0", "INSERVICESECTORCARRIERAPERSIS0");
		SHORT_MAP.put("inServiceSectorCarrierAPersist1", "INSERVICESECTORCARRIERAPERSIS1");
		SHORT_MAP.put("inServiceSectorCarrierAPersist2", "INSERVICESECTORCARRIERAPERSIS2");
		SHORT_MAP.put("inServiceSectorCarrierAPersist3", "INSERVICESECTORCARRIERAPERSIS3");
		SHORT_MAP.put("scGmsaMaxNumberOfQoSUsersAdmitted", "SCGMSAMAXNUMBEROFQOSUSERSADMIT");
		SHORT_MAP.put("inServiceSectorCarrierAPersist0", "INSERVICESECTORCARRIERAPERSIS0");
		SHORT_MAP.put("inServiceSectorCarrierAPersist1", "INSERVICESECTORCARRIERAPERSIS1");
		SHORT_MAP.put("inServiceSectorCarrierAPersist2", "INSERVICESECTORCARRIERAPERSIS2");
		SHORT_MAP.put("inServiceSectorCarrierAPersist3", "INSERVICESECTORCARRIERAPERSIS3");
		SHORT_MAP.put("scGmsaMaxNumberOfQoSUsersAdmitted", "SCGMSAMAXNUMBEROFQOSUSERSADMIT");

		String[] tableNames = null;
		if (isSPAS)
			tableNames = new String[]{"ds_clt_cm_luc_do_cell", "ds_clt_cm_luc_do_sector", "ds_clt_cm_luc_do_sectorcarrier",
					"ds_clt_cm_luc_do_fmsframe"};
		else
			tableNames = new String[]{"clt_cm_luc_do_fmsframe", "clt_cm_luc_do_profileid", "clt_cm_luc_do_tp", "clt_cm_luc_do_pdsn",
					"clt_cm_luc_do_diffservercode", "clt_cm_luc_do_vendor", "clt_cm_luc_do_ap", "clt_cm_luc_do_cdm", "clt_cm_luc_do_sector",
					"clt_cm_luc_do_cell", "clt_cm_luc_do_servicenode", "clt_cm_luc_do_servicenodeii", "clt_cm_luc_do_servicenodeiii",
					"clt_cm_luc_do_cellcabinet", "clt_cm_luc_do_aaaserver", "clt_cm_luc_do_ccmap", "clt_cm_luc_do_servicecategory",
					"clt_cm_luc_do_neighborsector", "clt_cm_luc_do_sectorcarrier", "clt_cm_luc_do_profileindex", "clt_cm_luc_do_ifhotarget"};

		if (isSPAS)
			TABLE_PREFIX = "DS_CLT_CM_LUC_DO_";

		Connection con = null;
		try {
			for (String name : tableNames) {
				try {
					String sql = "select * from " + name + " where 1=2";

					if (con == null || con.isClosed()) {
						con = DbPool.getConn();
					}
					Statement st = con.createStatement();
					ResultSet rs = st.executeQuery(sql);
					ResultSetMetaData meta = rs.getMetaData();
					List<String> cols = new ArrayList<String>();
					int count = meta.getColumnCount();
					for (int i = 0; i < count; i++) {
						String colName = meta.getColumnName(i + 1);
						if (!colName.equalsIgnoreCase("STAMPTIME") && !colName.equalsIgnoreCase("COLLECTTIME") && !colName.equalsIgnoreCase("OMCID"))
							cols.add(colName);
					}
					rs.close();
					st.close();
					st = null;
					rs = null;
					meta = null;
					cols.add(0, "OMCID");
					cols.add(1, "COLLECTTIME");
					cols.add(2, "STAMPTIME");
					TABLES_COLS.put(name.toUpperCase(), cols);
				} catch (Exception e) {
					logger.error("阿朗DO参数:读取clt表时异常：" + name, e);
				}
			}
		} catch (Exception e) {
			logger.error("阿朗DO参数:读取clt表时异常", e);
		} finally {
			CommonDB.close(null, null, con);
		}
	}

	public boolean parse(CollectObjInfo info, String fileName) {
		synchronized (CMParser.class) {

			collectTime = Util.getDateString(new Date());
			key = String.format("[taskId-%s][%s]", info.getTaskID(), Util.getDateString(info.getLastCollectTime()));
			omcId = String.valueOf(info.getDevInfo().getOmcID());
			if (isSPAS) {
				logger.debug("isSPAS=true");
				String nfileName = FilenameUtils.normalize(fileName);
				String zip = info.filenameMap.get(nfileName);
				if (zip == null || !zip.contains("_PARA_")) {
					logger.warn(info.getTaskID() + " 文件" + fileName + "，未找到对应的原始压缩包名。list=" + info.filenameMap);
				} else {
					zip = FilenameUtils.getBaseName(zip);
					String[] sp = zip.split("_");
					omcId = sp[4];
				}
			}
			stamptime = Util.getDateString(info.getLastCollectTime());
			this.info = info;
			logger.debug(key + "开始解析入库阿朗DO参数");
			File file = new File(fileName);
			InputStream in = null;
			XMLStreamReader reader = null;
			try {
				String tableName = null;
				Row row = null;
				boolean isFindEntity = false;
				in = new FileInputStream(file);
				reader = XMLInputFactory.newInstance().createXMLStreamReader(in);
				int type = -1;
				readyForSqlldr();
				while (reader.hasNext()) {
					type = reader.next();
					String tagName = null;

					if (type == XMLStreamConstants.START_ELEMENT || type == XMLStreamConstants.END_ELEMENT)
						tagName = reader.getLocalName();
					if (tagName == null) {
						continue;
					}
					switch (type) {
						case XMLStreamConstants.START_ELEMENT :
							if (tagName.equals("fld") || tagName.equals("keyfld")) {
								if (isFindEntity && tableName != null) {
									String col = reader.getAttributeValue(0);
									col = trimColName(col);
									String val = reader.getAttributeValue(1);
									if (TABLES_COLS.get(tableName).contains(col)) {
										row.addFiledValue(col, val);
									}
								}
							} else if (tagName.equals("entity")) {
								if (tableName != null) {
									isFindEntity = true;
									row = new Row(tableName);
								}
							} else if (tagName.equals("entitytype")) {
								String s = reader.getAttributeValue(0).toUpperCase();
								tableName = TABLE_PREFIX + s;
								if (tableName.equalsIgnoreCase("CLT_CM_LUC_DO_DIFFSERVERCODEPOINT"))
									tableName = "CLT_CM_LUC_DO_DIFFSERVERCODE";
								if (!TABLES_COLS.containsKey(tableName)) {
									tableName = null;
								}
							}
							break;
						case XMLStreamConstants.END_ELEMENT :
							if (tagName.equals("entity") && tableName != null) {
								isFindEntity = false;
								addForSqlldr(row);
								row = null;
							} else if (tagName.equals("entitytype")) {
								tableName = null;
							}
							break;
						default :
							break;
					}
				}
				doSqlldr();
			} catch (Exception e) {
				logger.error(key + "解析时异常", e);
				logger.debug(key + "阿朗DO参数解析入库失败");
				return false;
			} finally {
				sqlldrInfos.clear();
				if (reader != null) {
					try {
						reader.close();
					} catch (Exception unused) {
					}
				}
				if (in != null) {
					try {
						in.close();
					} catch (Exception unused) {
					}
				}
				System.gc();
			}
			logger.debug(key + "阿朗DO参数解析入库成功");
			return true;
		}
	}

	private void readyForSqlldr() throws Exception {
		Date now = new Date();
		String today = Util.getDateString_yyyyMMdd(now);
		// 存放阿朗DO参数数据临时文件的文件夹
		File dir = new File(SystemConfig.getInstance().getCurrentPath() + File.separator + "ldrlog" + File.separator + "al_do_cm" + File.separator
				+ today);
		if (!dir.exists()) {
			dir.mkdirs();
		}
		Iterator<String> it = TABLES_COLS.keySet().iterator();
		while (it.hasNext()) {
			String tn = it.next();
			List<String> cols = TABLES_COLS.get(tn);
			String mainFileName = info.getTaskID() + "_" + tn + "_" + Util.getDateString_yyyyMMddHHmmss(info.getLastCollectTime()) + "_"
					+ Util.getDateString_yyyyMMddHHmmssSSS(new Date());
			File ctl = new File(dir, mainFileName + ".ctl");
			File txt = new File(dir, mainFileName + ".txt");
			File log = new File(dir, mainFileName + ".log");
			File bad = new File(dir, mainFileName + ".bad");
			PrintWriter txtWriter = new PrintWriter(txt);
			SqlldrInfo sqlldrInfo = new SqlldrInfo(ctl, txt, log, bad, txtWriter);
			sqlldrInfos.put(tn, sqlldrInfo);
			PrintWriter ctlWriter = new PrintWriter(ctl);
			ctlWriter.println("load data");
			ctlWriter.println("CHARACTERSET " + SystemConfig.getInstance().getSqlldrCharset());
			ctlWriter.println("infile '" + txt.getAbsolutePath() + "' append into table " + tn);
			ctlWriter.println("FIELDS TERMINATED BY \";\"");
			ctlWriter.println("TRAILING NULLCOLS");
			ctlWriter.print("(OMCID,COLLECTTIME Date 'YYYY-MM-DD HH24:MI:SS',STAMPTIME Date 'YYYY-MM-DD HH24:MI:SS',");
			StringBuilder head = new StringBuilder();
			head.append("OMCID;COLLECTTIME;STAMPTIME;");
			for (int i = 3; i < cols.size(); i++) {
				String cn = cols.get(i);
				ctlWriter.print(cn);
				head.append(cn);
				if (i < cols.size() - 1) {
					ctlWriter.print(",");
					head.append(";");
				}
			}
			ctlWriter.print(")");
			ctlWriter.flush();
			ctlWriter.close();
			txtWriter.println(head);
			txtWriter.flush();
		}
	}

	private void addForSqlldr(Row row) {
		SqlldrInfo sqlldrInfo = sqlldrInfos.get(row.tableName);
		sqlldrInfo.txtWriter.println(row);
		sqlldrInfo.txtWriter.flush();
	}

	private void doSqlldr() throws Exception {
		Iterator<String> it = sqlldrInfos.keySet().iterator();
		while (it.hasNext()) {
			SqlldrInfo sqlldrInfo = sqlldrInfos.get(it.next());
			sqlldrInfo.txtWriter.flush();
			sqlldrInfo.txtWriter.close();
			String serviceName = SystemConfig.getInstance().getDbService();
			String uid = SystemConfig.getInstance().getDbUserName();
			String pwd = SystemConfig.getInstance().getDbPassword();
			String cmd = String.format("sqlldr userid=%s/%s@%s skip=1 control=%s bad=%s log=%s errors=9999999", uid, pwd, serviceName,
					sqlldrInfo.ctl.getAbsoluteFile(), sqlldrInfo.bad.getAbsoluteFile(), sqlldrInfo.log.getAbsoluteFile());
			ExternalCmd externalCmd = new ExternalCmd();
			logger.debug(key + "当前执行的SQLLDR命令为：" + cmd.replace(uid, "*").replace(pwd, "*"));
			int ret = -1;
			try {
				ret = externalCmd.execute(cmd);
			} catch (Exception e) {
				throw new StoreException(key + "执行sqlldr命令失败(" + cmd + ")", e);
			}
			SqlLdrLogAnalyzer analyzer = new SqlLdrLogAnalyzer();
			try {
				SqlldrResult result = analyzer.analysis(new FileInputStream(sqlldrInfo.log));
				if (result == null)
					return;

				logger.debug(key + " SQLLDR日志分析结果:ret=" + ret + "，omcid=" + omcId + " 入库成功条数=" + result.getLoadSuccCount() + " 表名="
						+ result.getTableName() + " 数据时间=" + stamptime + " sqlldr日志=" + sqlldrInfo.log.getAbsolutePath());

				dbLogger.log(Integer.parseInt(omcId), result.getTableName(), info.getLastCollectTime(), result.getLoadSuccCount(), info.getTaskID());
			} catch (Exception e) {
				logger.error(key + " sqlldr日志分析失败，文件名：" + sqlldrInfo.log.getAbsolutePath() + "，原因: ", e);
			}
			if (ret == 0 && SystemConfig.getInstance().isDeleteLog()) {
				sqlldrInfo.txt.delete();
				sqlldrInfo.ctl.delete();
				sqlldrInfo.bad.delete();
				sqlldrInfo.log.delete();
			}
		}
	}

	private String trimColName(String col) {
		String s = col;
		if (SHORT_MAP.containsKey(col))
			return SHORT_MAP.get(col);
		return s.toUpperCase();
	}

	/**
	 * 一行数据
	 * 
	 * @author ChenSijiang
	 */
	private class Row {

		List<String> cols;

		String tableName;

		private List<String> values = new ArrayList<String>();

		Row(String tableName) {
			this.tableName = tableName;
			cols = TABLES_COLS.get(tableName);
			for (int i = 0; i < cols.size(); i++)
				values.add("");
			values.set(0, omcId);
			values.set(1, collectTime);
			values.set(2, stamptime);
		}

		public void addFiledValue(String name, String value) {
			if (cols.contains(name)) {
				values.set(cols.indexOf(name), value);
			}
		}

		@Override
		public String toString() {
			StringBuilder line = new StringBuilder();
			for (int i = 0; i < values.size(); i++) {
				line.append(values.get(i));
				if (i < values.size() - 1) {
					line.append(";");
				}
			}
			return line.toString();
		}
	}

	private class SqlldrInfo {

		File ctl;

		File txt;

		File log;

		File bad;

		PrintWriter txtWriter;

		public SqlldrInfo(File ctl, File txt, File log, File bad, PrintWriter txtWriter) {
			super();
			this.ctl = ctl;
			this.txt = txt;
			this.log = log;
			this.bad = bad;
			this.txtWriter = txtWriter;
		}

	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		CollectObjInfo c = new CollectObjInfo(1122);
		DevInfo d = new DevInfo();
		d.setOmcID(808);
		c.setDevInfo(d);
		c.setLastCollectTime(new Timestamp(Util.getDate1("2012-02-21 00:00:00").getTime()));

		String fileName = "C:\\Users\\ChensSijiang\\Desktop\\sn191_201303280430CST.xml";
		new CMParser().parse(c, fileName);
	}

}
