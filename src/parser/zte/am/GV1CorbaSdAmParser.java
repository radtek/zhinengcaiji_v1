package parser.zte.am;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Connection;
import java.sql.PreparedStatement;

import org.apache.log4j.Logger;

import parser.Parser;
import util.CommonDB;
import util.DbPool;
import util.LogMgr;

/**
 * 山东中兴G网告警采集，方式是我方提供监听端口，中兴送数据上来。
 * 
 * @author ChenSijiang 2012-8-16
 */
public class GV1CorbaSdAmParser extends Parser {

	private static final Logger log = LogMgr.getInstance().getSystemLogger();

	static final String START_FLAG = "PACKET=";

	static final String END_FLAG = "END";

	static final String INSERT_SQL = "insert into clt_zte_g_am_test\n" + "(\n" + "PACKET      ,\n" + "MODULE      ,\n" + "ALARMID     ,\n"
			+ "MOUDLETYPE  ,\n" + "EVENTTYPE   ,\n" + "CAUSE       ,\n" + "EVENTTIME   ,\n" + "CANCELTIME  ,\n" + "SEVERITY    ,\n"
			+ "ALARMTEXT   ,\n" + "CODE        ,\n" + "SOURCE      ,\n" + "POSITION    ,\n" + "SOLUTION\n" + ")\n" + "values\n" + "(\n" + "?,\n"
			+ "?,\n" + "?,\n" + "?,\n" + "?,\n" + "?,\n" + "?,\n" + "?,\n" + "?,\n" + "?,\n" + "?,\n" + "?,\n" + "?,\n" + "?\n" + ")";

	long taskId;

	int lisPort;

	ServerSocket server;

	Socket client;

	InputStream in;

	InputStreamReader reader;

	BufferedReader br;

	@Override
	public boolean parseData() throws Exception {
		this.taskId = getCollectObjInfo().getTaskID();
		this.lisPort = getCollectObjInfo().getDevPort();

		if (!openServer())
			return false;

		log.debug(taskId + " 监听端口已开启，等待对方连接：" + this.server);
		this.client = this.server.accept();
		log.debug(taskId + " 接收到了一个客户端连接：" + this.client);
		this.in = this.client.getInputStream();
		this.reader = new InputStreamReader(this.in);
		this.br = new BufferedReader(this.reader);
		this.handleData();
		return true;
	}

	void handleData() throws Exception {
		String line = null;
		StringBuilder buff = new StringBuilder();
		boolean bStart = false;
		while ((line = this.br.readLine()) != null) {
			line = line.trim();
			if (line.startsWith(START_FLAG) && !bStart) {
				bStart = true;
			} else if (line.equals(END_FLAG) && bStart) {
				buff.append(line);
				bStart = false;
				Zte2GRaw entry = Zte2GRaw.fromRaw(buff.toString());
				if (entry != null) {
					log.debug(taskId + " 接收到一条完整记录：" + entry);
					insert(entry);
				} else {
					log.debug(taskId + " 记录解析未成，原始：" + buff);
				}
				buff.setLength(0);
			}
			if (bStart)
				buff.append(line).append("\n");
		}
	}

	boolean openServer() {
		try {
			this.server = new ServerSocket(this.lisPort);
		} catch (IOException e) {
			log.error(taskId + " 监听端口创建失败：" + this.lisPort, e);
			return false;
		}
		return true;
	}

	void insert(Zte2GRaw entry) {
		Connection con = null;
		PreparedStatement ps = null;
		try {
			con = DbPool.getConn();
			ps = con.prepareStatement(INSERT_SQL);
			ps.setString(1, entry.PACKET);
			ps.setString(2, entry.MODULE);
			ps.setString(3, entry.ALARMID);
			ps.setString(4, entry.MOUDLETYPE);
			ps.setString(5, entry.EVENTTYPE);
			ps.setString(6, entry.CAUSE);
			ps.setTimestamp(7, entry.EVENTTIME);
			ps.setTimestamp(8, entry.CANCELTIME);
			ps.setString(9, entry.SEVERITY);
			ps.setString(10, entry.ALARMTEXT);
			ps.setString(11, entry.CODE);
			ps.setString(12, entry.SOURCE);
			ps.setString(13, entry.POSITION);
			ps.setString(14, entry.SOLUTION);
			ps.executeUpdate();
			log.debug(taskId + " 记录入库成功。");
		} catch (Exception e) {
			log.error(taskId + " 插入数据失败：" + entry, e);
		} finally {
			CommonDB.close(null, ps, con);
		}
	}

}
