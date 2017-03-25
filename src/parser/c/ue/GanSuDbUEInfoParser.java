package parser.c.ue;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Logger;

import parser.Parser;
import task.CollectObjInfo;
import util.CommonDB;
import util.DbPool;
import util.LogMgr;
import framework.SystemConfig;

/**
 * 甘肃电信终端信息。
 * 
 * @author ChensSijiang 2013-2-18
 */
public class GanSuDbUEInfoParser extends Parser {

	private static final Logger log = LogMgr.getInstance().getSystemLogger();

	private static final String MODEL_TABLE = "clt_terminal_tacinfo";

	private Map<ModelInfo, Short> modelMap = new HashMap<ModelInfo, Short>();

	// private String dburl;// jdbc:oracle:thin:@132.228.39.128:1521:ORCL

	// private String dbDriver;

	// private String user; // js

	// private String pwd;// js

	private String sql;

	private Connection con;

	private ResultSet rs;

	private Statement st;

	private long taskid;

	private Map<String/* 文件绝对路径 */, RandomAccessFile> writers = new HashMap<String, RandomAccessFile>();

	private File basedir;

	public GanSuDbUEInfoParser(CollectObjInfo task) {
		collectObjInfo = task;
	}

	@Override
	public boolean parseData() throws Exception {
		this.taskid = getCollectObjInfo().getTaskID();
		this.sql = getCollectObjInfo().getCollectPath();
		log.debug(taskid + " 开始采集终端信息（生成ILAP索引文件），配置的SQL语句为：" + this.sql);
		loadModelMap();
		try {
			initWriters();
			log.debug(taskid + " 目录初始化完成，基准目录：" + basedir);
		} catch (Exception e) {
			log.error(taskid + " 目录初始化失败，任务结束。dir：" + basedir, e);
			return false;
		}
		try {
			con = CommonDB.getConnection(getCollectObjInfo(), 3000, (byte) 3);
			log.debug(taskid + " 连接获取成功：" + con);
			log.debug(taskid + " 开始发送SQL语句：" + sql);
			st = con.createStatement();
			rs = st.executeQuery(this.sql);
			log.debug(taskid + " SQL语句已响应，开始写入索引文件：" + sql);
			handleRs();
		} catch (Exception e) {
			log.error(taskid + " 采集终端信息（生成ILAP索引文件），执行失败。", e);
			return false;
		} finally {
			CommonDB.close(rs, st, con);
			log.debug(taskid + " 开始关闭索引文件。");
			Iterator<String> it = writers.keySet().iterator();
			while (it.hasNext()) {
				String file = it.next();
				RandomAccessFile raf = writers.get(file);
				if (raf != null)
					raf.close();
			}
			writers.clear();
			log.debug(taskid + " 索引文件关闭结束。");
		}
		return true;
	}

	static boolean isBlank(String string) {
		return string == null || "".equals(string.trim());
	}

	boolean validate(String vendor, String termodel) {
		return isBlank(vendor) || isBlank(termodel);
	}

	void handleRs() throws Exception {
		int count = 0;
		while (rs.next()) {
			count++;

			short imsiPart1 = 0;
			short imsiPart2 = 0;
			short imsiPart3 = 0;
			String shortImsi = null;
			String imsi = null;
			try {
				imsi = rs.getString("imsi");
				String vendor = rs.getString("terminal_firm");
				String termmodel = rs.getString("mobile_model");
				if (validate(vendor, termmodel)) {
					continue;
				}
				imsi = imsi.trim();
				vendor = vendor.trim();
				termmodel = termmodel.trim();
				shortImsi = imsi.substring(5);
				imsiPart1 = Short.parseShort(shortImsi.substring(0, 2));
				imsiPart2 = Short.parseShort(shortImsi.substring(2, 6));
				imsiPart3 = Short.parseShort(shortImsi.substring(6, 10));
				ModelInfo mi = new ModelInfo((short) 0, vendor, termmodel);
				short typeid = -1;
				if (this.modelMap.containsKey(mi)) {
					Short s = this.modelMap.get(mi);
					if (s != null)
						typeid = s;
				} else {
					if (this.addModel(mi)) {
						Short s = this.modelMap.get(mi);
						if (s != null)
							typeid = s;
					}
				}

				if (typeid > -1) {
					File bin = new File(basedir.getAbsolutePath() + File.separator + "IMSI_INDEX" + File.separator + wrap2(imsiPart1)
							+ File.separator + wrap4(imsiPart2) + ".bin");
					RandomAccessFile raf = null;
					if (writers.containsKey(bin.getAbsolutePath())) {
						raf = writers.get(bin.getAbsolutePath());
					} else {
						bin.getParentFile().mkdirs();
						raf = new RandomAccessFile(bin, "rw");
						writers.put(bin.getAbsolutePath(), raf);
					}
					/* 每个数据单元占2字节，所以每次seek要乘以2. */
					raf.seek(imsiPart3 * 2);
					writeSwappedShort(raf, typeid);
				}

				if (count % 10000 == 0)
					log.debug(taskid + " 处理了" + count + "条记录.");

			} catch (NumberFormatException e) {
				continue;
			}
		}
	}

	boolean addModel(ModelInfo mi) {
		String sql = "insert into " + MODEL_TABLE + " (tactypeid,tac_vendor,mobile_model) values (" + "(select nvl(max(tactypeid),0)+1 from "
				+ MODEL_TABLE + "),?,?)";
		Connection conn = null;
		PreparedStatement pss = null;
		try {
			conn = DbPool.getConn();
			pss = conn.prepareStatement(sql);
			pss.setString(1, mi.vendor);
			pss.setString(2, mi.model);
			pss.executeUpdate();
		} catch (Exception e) {
			log.error(taskid + " 添加终端型号编号表失败，SQL：" + sql + "，mi：" + mi, e);
			return false;
		} finally {
			CommonDB.close(null, pss, conn);
		}
		loadModelMap();
		log.debug(taskid + " 成功添加了一条新终端型号： " + mi);
		return true;
	}

	void loadModelMap() {
		String sql = "select tac_vendor,mobile_model,max(tactypeid) as tactypeid from " + MODEL_TABLE + " group by tac_vendor,mobile_model";
		Connection conn = null;
		ResultSet rss = null;
		Statement stt = null;
		this.modelMap.clear();
		try {
			conn = DbPool.getConn();
			stt = conn.createStatement();
			// log.debug(taskid + " 查询终端编号表：" + sql);
			rss = stt.executeQuery(sql);
			while (rss.next()) {
				ModelInfo mi = new ModelInfo(rss.getShort("tactypeid"), rss.getString("tac_vendor"), rss.getString("mobile_model"));
				this.modelMap.put(mi, mi.id);
			}
		} catch (Exception e) {
			log.error(taskid + " 查询终端编号表失败：" + sql, e);
		} finally {
			CommonDB.close(rss, stt, conn);
		}
	}

	private void initWriters() throws Exception {
		basedir = new File(SystemConfig.getInstance().getCurrentPath() + File.separator + "cdma_ue_info" + File.separator + collectObjInfo.getKeyID()
				+ File.separator);
		if (basedir.exists())
			return;
		if (!basedir.mkdirs()) {
			throw new Exception("创建数据目录失败 - " + basedir);
		}
	}

	/* 数字不够2位的，前面补零。 */
	private static String wrap2(short val) {
		if (val > 9)
			return String.valueOf(val);
		return "0" + val;
	}

	/* 数字不够3位的，前面补零。 */
	private static String wrap3(short val) {
		if (val > 99)
			return String.valueOf(val);
		return "0" + wrap2(val);
	}

	/* 数字不够4位的，前面补零。 */
	private static String wrap4(short val) {
		if (val > 999)
			return String.valueOf(val);
		return "0" + wrap3(val);
	}

	static void writeSwappedShort(RandomAccessFile output, short value) throws IOException {
		output.write((byte) ((value >> 0) & 0xff));
		output.write((byte) ((value >> 8) & 0xff));
	}
}
