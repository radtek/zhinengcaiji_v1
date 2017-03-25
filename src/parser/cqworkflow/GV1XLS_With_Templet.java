package parser.cqworkflow;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import jxl.Cell;
import jxl.Sheet;
import jxl.Workbook;

import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import parser.Parser;
import task.CollectObjInfo;
import task.DevInfo;
import util.CommonDB;
import util.DbPool;
import util.Util;
import framework.SystemConfig;

/**
 * G网重庆工单配置，EXCEL文件部分。
 * 
 * @author ChenSijiang 2011-2-24 下午06:45:01
 */
public class GV1XLS_With_Templet extends Parser {

	private String logKey;

	private String stamptime;

	private String omcid;

	private Timestamp tsStamptime;

	private Timestamp tsCollecttime;

	@Override
	public boolean parseData() throws Exception {
		this.stamptime = Util.getDateString(collectObjInfo.getLastCollectTime());
		this.tsCollecttime = new Timestamp(System.currentTimeMillis());
		this.tsStamptime = new Timestamp(collectObjInfo.getLastCollectTime().getTime());
		this.omcid = String.valueOf(collectObjInfo.getDevInfo().getOmcID());
		this.logKey = String.format("[%s][%s]", collectObjInfo.getTaskID(), this.stamptime);
		XlsTemplet templet = parseTemplet(collectObjInfo.getParseTmpID());
		String sql = createSQLStatement(templet);
		try {
			File xlsFile = new File(fileName);
			Workbook xls = Workbook.getWorkbook(xlsFile);

			Sheet sheet = xls.getSheet(0);
			int rowCount = sheet.getRows();

			if (rowCount < 1)
				throw new Exception("excel表格的行数为0");

			Map<Integer, String> colIndex = new HashMap<Integer, String>();

			Cell[] firstLine = sheet.getRow(0);
			for (int i = 0; i < firstLine.length; i++) {
				String content = firstLine[i].getContents();
				content = content == null ? "" : content.trim();

				XlsHead h = templet.findColByHead(content);

				colIndex.put(i, content);

			}

			for (int i = 1; i < rowCount; i++) {
				Cell[] cells = sheet.getRow(i);
				insert(cells, sql);

			}

			return true;
		} catch (Exception e) {
			log.error(logKey + "解析文件出错", e);
			return false;
		}

	}

	Connection con = null;

	PreparedStatement ps = null;

	private void insert(Cell[] cells, String sql) {
		try {
			if (con == null)
				con = DbPool.getConn();
			if (ps == null)
				ps = con.prepareStatement(sql);

			ps.setString(1, this.omcid);
			ps.setTimestamp(2, this.tsCollecttime);
			ps.setTimestamp(3, this.tsStamptime);

			int index = 4;

			for (int i = 0; i < cells.length; i++) {
				String content = cells[i].getContents();

			}
		} catch (Exception e) {
			log.error(logKey + "入库时出现异常");
		}
	}

	private String createSQLStatement(XlsTemplet templet) {

		StringBuilder buffer = new StringBuilder();

		buffer.append("insert into ").append(templet.tableName).append(" (OMCID,COLLECTTIME,STAMPTIME,");

		for (int i = 0; i < templet.heads.size(); i++) {
			buffer.append(templet.heads.get(i).col);
			if (i < templet.heads.size() - 1)
				buffer.append(",");
		}
		buffer.append(") values (?,?,?,");
		for (int i = 0; i < templet.heads.size(); i++) {
			buffer.append("?");
			if (i < templet.heads.size() - 1)
				buffer.append(",");
		}
		buffer.append(")");
		String s = buffer.toString();
		buffer.setLength(0);
		return s;
	}

	private XlsTemplet parseTemplet(int tmpid) {
		String sql = "select tempfilename from igp_conf_templet where tmpid=" + tmpid;

		Connection con = null;
		Statement st = null;
		ResultSet rs = null;
		try {
			con = DbPool.getConn();
			st = con.createStatement();
			rs = st.executeQuery(sql);
			String filename = null;
			if (rs.next())
				filename = rs.getString(1);
			else
				throw new Exception("未查询到模板记录 - " + sql);

			if (Util.isNull(filename))
				throw new Exception("模板文件名为空 - " + sql);

			File file = new File(SystemConfig.getInstance().getTempletPath(), filename.trim());
			if (!file.exists())
				throw new Exception("模板文件不存在 - " + file.getAbsolutePath());
			Document doc = new SAXReader().read(file);

			Element root = doc.getRootElement();
			String tn = root.attributeValue("table");
			List<Element> headList = root.elements("head");
			List<XlsHead> heads = new ArrayList<XlsHead>();
			int index = 0;
			for (Element e : headList)
				heads.add(new XlsHead(e.attributeValue("name"), e.attributeValue("col"), e.attributeValue("type"), e.attributeValue("format"), tn,
						index++));

			return new XlsTemplet(tn, heads);
		} catch (Exception e) {
			log.error(logKey + "读取模板时出错", e);
			return null;
		} finally {
			CommonDB.closeDBConnection(con, st, rs);
		}

	}

	private class XlsTemplet {

		String tableName;

		List<XlsHead> heads;

		public XlsTemplet(String tableName, List<XlsHead> heads) {
			super();
			this.tableName = tableName;
			this.heads = heads;
		}

		public XlsHead findColByHead(String head) {
			for (XlsHead h : heads) {
				if (h.name.equals(head))
					return h;
			}
			return null;
		}

	}

	private class XlsHead {

		String name;

		String col;

		String type;

		String format;

		String tableName;

		int index;

		public XlsHead(String name, String col, String type, String format, String tableName, int index) {
			super();
			this.name = name;
			this.col = col;
			this.type = type;
			this.format = format;
			this.tableName = tableName;
			this.index = index;
		}
	}

	public static void main(String[] args) {
		CollectObjInfo obj = new CollectObjInfo(22501);
		DevInfo dev = new DevInfo();
		dev.setOmcID(111);
		obj.setParseTmpID(2250101);
		obj.setDevInfo(dev);
		obj.setLastCollectTime(new Timestamp(new Date().getTime()));

		GV1XLS_With_Templet w = new GV1XLS_With_Templet();
		w.fileName = "F:\\ftp_root\\cq\\4\\CQBTSConfig\\1297413322410.xls";
		w.collectObjInfo = obj;
		try {
			w.parseData();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
