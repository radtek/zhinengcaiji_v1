package parser.boco.am;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import parser.Parser;
import util.CommonDB;
import util.LogMgr;
import util.Util;
import framework.SystemConfig;

/**
 * 江西联通亿阳网管硬件告警采集。
 * 
 * @author ChenSijiang 2012-9-12
 */
public class JiangXiAlarm2Parser extends Parser {

	private static final Logger log = LogMgr.getInstance().getSystemLogger();

	static final String START_FLAG = "<AlarmStart>";

	static final String END_FLAG = "<AlarmEnd>";

	long taskId;

	int lisPort;

	ServerSocket server;

	Templet templet;

	Connection gsmCon;

	Connection wcdmaCon;

	private long count = 0;

	public boolean parseData() throws Exception {
		this.taskId = getCollectObjInfo().getTaskID();
		this.lisPort = getCollectObjInfo().getDevPort();
		String ip = getCollectObjInfo().getDevInfo().getIP();

		this.templet = new Templet();
		try {
			this.templet.parse(this.getCollectObjInfo().getParseTmpID(), this.getCollectObjInfo().getDevInfo().getOmcID());
		} catch (Exception e) {
			log.error(taskId + " 读取采集模板失败。", e);
			return false;
		}

		try {
			if (!openCon())
				return false;
			
			log.debug(taskId + "[" + getCollectObjInfo().getDescribe() + "] 开始连接：" + ip + ", 端口：" + this.lisPort);
			
			Socket client = new Socket(ip, this.lisPort);
			client.setSoTimeout(10 * 1000);//超时十秒钟
			Work work = new Work(this, client);
			work.start();
			return true;
		} finally {
			CommonDB.close(null, null, this.wcdmaCon);
			CommonDB.close(null, null, this.gsmCon);
			try {
				if (server != null)
					server.close();
			} catch (Exception e) {
			}
		}

	}

	synchronized void insert(Map<String, String> attributes) {
		String sql = null;
		Connection con = null;
		List<TField> fields = null;
		String omc_id = attributes.get("omc_id");
		if (omc_id == null)
			omc_id = "";
		if (!this.templet.gsmOmcIdExcludeList.isEmpty()) {
			if (!this.templet.gsmOmcIdExcludeList.contains(omc_id)) {
				sql = this.templet.gsmSQL;
				con = this.gsmCon;
				fields = this.templet.gsmFields;
			}
		}
		if (!this.templet.gsmOmcIdIncludeList.isEmpty()) {
			if (this.templet.gsmOmcIdIncludeList.contains(omc_id)) {
				sql = this.templet.gsmSQL;
				con = this.gsmCon;
				fields = this.templet.gsmFields;
			}
		}
		if (!this.templet.wcdmaOmcIdExcludeList.isEmpty()) {
			if (!this.templet.wcdmaOmcIdExcludeList.contains(omc_id)) {
				sql = this.templet.wcdmaSQL;
				con = this.wcdmaCon;
				fields = this.templet.wcdmaFields;
			}
		}
		if (!this.templet.wcdmaOmcIdIncludeList.isEmpty()) {
			if (this.templet.wcdmaOmcIdIncludeList.contains(omc_id)) {
				sql = this.templet.wcdmaSQL;
				con = this.wcdmaCon;
				fields = this.templet.wcdmaFields;
			}
		}

		if (sql == null || con == null) {
			log.debug(taskId + " 由于omc_id(" + omc_id + ")找不到属于wcdma还是gsm，所以丢弃：" + attributes);
			return;
		}

		PreparedStatement ps = null;
		try {
			ps = con.prepareStatement(sql);
			for (int i = 0; i < fields.size(); i++) {
				String name = fields.get(i).name;
				String value = attributes.get(name);
				if (!fields.get(i).isDate())
					ps.setString(i + 1, value);
				else
					ps.setTimestamp(i + 1, _toTs(value, fields.get(i).dateFormat));
			}
			if (ps.executeUpdate() > 0)
				count++;
		} catch (Exception e) {
			log.debug(taskId + " 记录入库失败：" + attributes, e);

			if (e instanceof SQLException && e.toString().toLowerCase().indexOf("close") != -1) {
				CommonDB.close(null, null, this.wcdmaCon);
				CommonDB.close(null, null, this.gsmCon);
				log.info(taskId + "gsm/wcdma 数据库关闭连接");
				log.debug(taskId + " 休息30秒，重新打开数据库连接");
				try {
					Thread.sleep(1000 * 30);
				} catch (InterruptedException e1) {
				}

				openCon();
			}

		} finally {
			CommonDB.close(null, ps, null);
		}
	}

	void parseAttributes(Map<String, String> outMap, String content) {
		if (outMap == null || Util.isNull(content))
			return;

		String[] sp0 = content.split("\n");
		for (String s0 : sp0) {
			String[] sp1 = s0.split(":", 2);
			String name = "";
			String value = "";
			if (sp1 != null) {
				if (sp1.length > 0)
					name = sp1[0];
				if (sp1.length > 1)
					value = sp1[1];
				if (value != null)
					value = value.trim();
			}
			if (Util.isNotNull(name))
				outMap.put(name.toLowerCase().trim(), value);
		}

		// alarmtext拆为alarmtext_1和alarmtext_2两列。
		if (outMap.containsKey("alarmtext")) {
			String at = outMap.remove("alarmtext");
			if (at != null)
				at = at.replace("&gt;", "").replace("&lt;", "").replace("++", "").replace("--", "").replace("&amp;", "");
			if (Util.isNotNull(at) && at.length() > 800) {
				if (at.length() > 800 * 2)
					at = at.substring(0, 800 * 2);
				String a1 = at.substring(0, 800);
				String a2 = at.substring(800, at.length());
				outMap.put("alarmtext_1", a1);
				outMap.put("alarmtext_2", a2);
			} else {
				outMap.put("alarmtext_1", at);
				outMap.put("alarmtext_2", "");
			}
		}
	}

	boolean openServer() {
		try {
			this.server = new ServerSocket(this.lisPort);
			this.server.setSoTimeout(10 * 60 * 1000);
		} catch (IOException e) {
			log.error(taskId + " 监听端口创建失败：" + this.lisPort, e);
			return false;
		}
		return true;
	}

	boolean openCon() {
		this.gsmCon = CommonDB.getConnection(this.taskId, SystemConfig.getInstance().getDbDriver(), this.templet.gsmDbUrl, this.templet.gsmDbUser,
				this.templet.gsmDbPwd);
		this.wcdmaCon = CommonDB.getConnection(this.taskId, SystemConfig.getInstance().getDbDriver(), this.templet.wcdmaDbUrl,
				this.templet.wcdmaDbUser, this.templet.wcdmaDbPwd);
		if (this.gsmCon == null || this.wcdmaCon == null) {
			CommonDB.close(null, null, this.wcdmaCon);
			CommonDB.close(null, null, this.gsmCon);
			log.error(taskId + "gsm/wcdma 数据库连接打开失败。");
			return false;
		}
		return true;
	}

	Timestamp _toTs(String val, String fmt) {
		try {
			return new Timestamp(Util.getDate(val, fmt).getTime());
		} catch (Exception e) {
			return null;
		}
	}

	static public class Work extends Thread {

		private JiangXiAlarm2Parser parser = null;

		private Socket client;

		public Work(JiangXiAlarm2Parser parser, Socket client) {
			this.parser = parser;
			this.client = client;
		}

		@Override
		public void run() {
			InputStream in = null;
			InputStreamReader reader = null;
			BufferedReader br = null;

			try {
//				client.setSoTimeout(60 * 1000 * 30);
				log.debug(parser.taskId + " 开始读取数据：" + client);

				in = client.getInputStream();
				if (Util.isNotNull(parser.getCollectObjInfo().getDevInfo().getEncode()))
					reader = new InputStreamReader(in, parser.getCollectObjInfo().getDevInfo().getEncode());
				else
					reader = new InputStreamReader(in);
				br = new BufferedReader(reader);

				String line = null;
				StringBuilder buff = new StringBuilder();
				Map<String, String> attributes = new HashMap<String, String>();
				boolean bStart = false;
				while ((line = br.readLine()) != null) {

					line = line.trim();
					if (bStart) {
						if (line.equalsIgnoreCase(END_FLAG)) {
							attributes.clear();
							parser.parseAttributes(attributes, buff.toString());
							if (!attributes.isEmpty())
								parser.insert(attributes);
							log.debug(parser.taskId + " 目前已入库了" + parser.count + "条告警数据。");
							bStart = false;
						} else {
							buff.append(line).append("\n");
						}
					} else if (line.equalsIgnoreCase(START_FLAG)) {
						buff.setLength(0);
						bStart = true;
					}
				}
			} catch (SocketTimeoutException e) {
				log.debug(parser.taskId + " 数据读取完毕");
			} catch (Exception e) {
				log.debug(parser.taskId + " 处理发生异常退出", e);
			} finally {
				IOUtils.closeQuietly(br);
				IOUtils.closeQuietly(reader);
				IOUtils.closeQuietly(in);
				if (client != null) {
					try {
						client.close();
					} catch (Exception e) {

					}
				}
			}
		}

	}
}
