package parser.c.lossdata;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import parser.Parser;
import task.CollectObjInfo;
import task.DevInfo;
import util.CommonDB;
import util.DbPool;
import util.LogMgr;
import util.Util;

public class LossDataParser extends Parser {

	private Timestamp stamptime;

	private Timestamp collecttime;

	private String strStamptime;

	private long taskId;

	private int omcId;

	private String logKey;

	private int insertCount;

	private Connection connection;

	private PreparedStatement ps;

	private static final String TABLE_NAME = "CLT_MISSBSA";

	private static final String SQL_INSERT = "insert into " + TABLE_NAME + "\n" + "  (omcid, collecttime, stamptime, sid, nid, exbsid)\n"
			+ "values\n" + "  (?, ?, ?, ?, ?, ?)";

	private static final String SEPARATOR = " ";

	private static final Logger logger = LogMgr.getInstance().getSystemLogger();

	@Override
	public boolean parseData() throws Exception {
		this.taskId = collectObjInfo.getTaskID();
		this.stamptime = collectObjInfo.getLastCollectTime();
		this.collecttime = new Timestamp(System.currentTimeMillis());
		this.strStamptime = Util.getDateString(this.stamptime);
		this.omcId = collectObjInfo.getDevInfo().getOmcID();
		this.logKey = String.format("[%s][%s]", this.taskId, this.strStamptime);

		File parseFile = new File(fileName);

		InputStream in = null;
		InputStreamReader isr = null;
		BufferedReader br = null;
		String line = null;
		try {
			in = new FileInputStream(parseFile);
			isr = new InputStreamReader(in);
			br = new BufferedReader(isr);
			while ((line = br.readLine()) != null) {
				if (Util.isNull(line))
					continue;

				Integer sid = null;
				Integer nid = null;
				String exBsId = null;

				String[] sp1 = line.split(SEPARATOR);
				if (sp1 == null || sp1.length == 0) {
					logger.warn(logKey + "不正确的数据行：" + line + "，文件：" + fileName);
					continue;
				}
				for (String s : sp1) {
					String[] sp2 = s.split("=");
					if (sp2 == null || sp2.length == 0) {
						logger.warn(logKey + "不正确的数据行：" + line + "，文件：" + fileName);
						continue;
					} else {
						if (sp2[0].trim().equalsIgnoreCase("SID")) {
							if (sp2.length > 1)
								sid = Integer.valueOf(sp2[1]);
						} else if (sp2[0].trim().equalsIgnoreCase("NID")) {
							if (sp2.length > 1)
								nid = Integer.valueOf(sp2[1]);
						} else if (sp2[0].trim().equalsIgnoreCase("ExBSID")) {
							if (sp2.length > 1)
								exBsId = sp2[1];
						}
					}
				}
				insert(sid, nid, exBsId);
			}
			LogMgr.getInstance().getDBLogger().log(this.omcId, TABLE_NAME, this.stamptime, insertCount, this.taskId);
		} catch (Exception e) {
			logger.error(logKey + "解析文件时异常，文件：" + fileName + "，当前行：" + line, e);
			return false;
		} finally {
			CommonDB.close(null, ps, connection);
			ps = null;
			connection = null;
			insertCount = 0;
			IOUtils.closeQuietly(br);
			IOUtils.closeQuietly(isr);
			IOUtils.closeQuietly(in);
		}

		return true;
	}

	private void insert(Integer sid, Integer nid, String exBsId) {
		try {
			if (connection == null) {
				connection = DbPool.getConn();
			}
			ps = connection.prepareStatement(SQL_INSERT);

			ps.setInt(1, this.omcId);
			ps.setTimestamp(2, this.collecttime);
			ps.setTimestamp(3, this.stamptime);
			ps.setInt(4, sid);
			ps.setInt(5, nid);
			ps.setString(6, exBsId);
			if (ps.executeUpdate() > 0)
				insertCount++;
		} catch (Exception e) {
			logger.error(logKey + "插入数据时异常，sql=" + SQL_INSERT + "，sid=" + sid + ",nid=" + nid + ",exBsId=" + exBsId, e);
		} finally {
			CommonDB.close(null, ps, null);
		}
	}

	public static void main(String[] args) throws Throwable {
		CollectObjInfo obj = new CollectObjInfo(755123);
		DevInfo dev = new DevInfo();
		dev.setOmcID(302);
		obj.setDevPort(21);
		obj.setDevInfo(dev);
		obj.setLastCollectTime(new Timestamp(Util.getDate1("2011-07-06 20:00:00").getTime()));
		LossDataParser p = new LossDataParser();
		p.setCollectObjInfo(obj);
		p.setFileName("C:\\Documents and Settings\\ChenSijiang\\桌面\\Guangdong_missbsa_20110601.txt");
		p.parseData();
	}
}
