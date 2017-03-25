package parser.cqworkflow;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.Date;

import jxl.Cell;
import jxl.Sheet;
import jxl.Workbook;
import parser.Parser;
import task.CollectObjInfo;
import task.DevInfo;
import util.CommonDB;
import util.DbPool;
import util.LogMgr;
import util.Util;

/**
 * G网重庆工单配置，EXCEL文件部分。
 * 
 * @author ChenSijiang 2011-2-24 下午06:45:01
 */
public class GV1XLS extends Parser {

	private String logKey;

	private String stamptime;

	private String omcid;

	private Timestamp tsStamptime;

	private Timestamp tsCollecttime;

	private int insertCount = 0;

	private static final String SQL = "insert into CLT_TASKLIST  (OMCID,COLLECTTIME,STAMPTIME,"
			+ "START_TIME,CHINA_NAME,REMARK,CI,MAX_CARRIER_NUM,MAX_AVAIL_CARRIER,MAX_TRAFFIC,"
			+ "SUM_TRAFFIC,MAX_TRAFFIC_HALF,SUM_TRAFFIC_HALF,MAX_CALL_BLOCK,SUM_CALL_BLOCK,MAX_CALL_ATT,"
			+ "SUM_CALL_ATT,SUM_CALL_SEIZE,SUM_CALL_DROP) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?," + " ?, ?, ?, ?, ?, ?, ?)";

	@Override
	public boolean parseData() throws Exception {
		insertCount = 0;
		this.stamptime = Util.getDateString(collectObjInfo.getLastCollectTime());
		this.tsCollecttime = new Timestamp(System.currentTimeMillis());
		this.tsStamptime = new Timestamp(collectObjInfo.getLastCollectTime().getTime());
		this.omcid = String.valueOf(collectObjInfo.getDevInfo().getOmcID());
		this.logKey = String.format("[%s][%s]", collectObjInfo.getTaskID(), this.stamptime);
		try {
			File xlsFile = new File(fileName);
			log.debug(logKey + "开始解析 - " + xlsFile.getAbsolutePath());
			Workbook xls = Workbook.getWorkbook(xlsFile);

			Sheet sheet = xls.getSheet(0);
			int rowCount = sheet.getRows();

			if (rowCount < 1)
				throw new Exception("excel表格的行数为0");

			for (int i = 1; i < rowCount; i++) {
				Cell[] cells = sheet.getRow(i);
				insert(cells);

			}
			log.debug(logKey + "解析完成 - " + xlsFile.getAbsolutePath());
			return true;
		} catch (Exception e) {
			log.error(logKey + "解析文件出错", e);
			return false;
		} finally {
			CommonDB.closeDBConnection(con, ps, null);
			con = null;
			ps = null;
			LogMgr.getInstance().getDBLogger()
					.log(collectObjInfo.getDevInfo().getOmcID(), "CLT_TASKLIST", tsStamptime, insertCount, collectObjInfo.getTaskID());
		}

	}

	Connection con = null;

	PreparedStatement ps = null;

	private synchronized void insert(Cell[] cells) {
		try {
			if (con == null)
				con = DbPool.getConn();
			if (ps == null)
				ps = con.prepareStatement(SQL);

			ps.setString(1, this.omcid);
			ps.setTimestamp(2, this.tsCollecttime);
			ps.setTimestamp(3, this.tsStamptime);

			int index = 4;

			for (int i = 0; i < cells.length; i++) {
				String content = cells[i].getContents();
				if (i == 0) {
					Timestamp ts = null;
					try {
						ts = new Timestamp(Util.getDate(content, "yyyy-MM-dd").getTime());
					} catch (Exception e) {
					}
					ps.setTimestamp(index++, ts);
				} else {
					ps.setString(index++, content);
				}
			}
			if (ps.executeUpdate() > 0)
				insertCount++;
		} catch (Exception e) {
			log.error(logKey + "入库时出现异常", e);
		}
	}

	public static void main(String[] args) {
		CollectObjInfo obj = new CollectObjInfo(22501);
		DevInfo dev = new DevInfo();
		dev.setOmcID(111);
		obj.setParseTmpID(2250101);
		obj.setDevInfo(dev);
		obj.setLastCollectTime(new Timestamp(new Date().getTime()));

		GV1XLS w = new GV1XLS();
		w.fileName = "F:\\ftp_root\\cq\\4\\CQBTSConfig\\1297413322410.xls";
		w.collectObjInfo = obj;
		try {
			w.parseData();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
