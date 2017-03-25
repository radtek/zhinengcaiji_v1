package parser.siemens.cm;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.io.SAXReader;

import parser.siemens.cm.TempletConfig.Property;
import parser.siemens.cm.TempletConfig.Table;
import task.CollectObjInfo;
import task.DevInfo;
import util.CommonDB;
import util.DBLogger;
import util.DbPool;
import util.LogMgr;
import util.Util;
import framework.ConstDef;
import framework.SystemConfig;

/**
 * G网西门子参数，文件方式解析。
 * 
 * @author ChenSijiang 2010.07.09
 * @since 3.1
 */
class SiemensCmParser {

	private long taskId;

	private int omcid;

	private Timestamp stampTime;

	private int bscId;

	private TempletConfig tc; // 模板信息

	private Map<String, List<Record>> tableNameToRecords = new HashMap<String, List<Record>>(); // 表名，及对应的记录

	private Logger logger = LogMgr.getInstance().getSystemLogger();

	private DBLogger dbLogger = LogMgr.getInstance().getDBLogger();

	private Map<String, Integer> count = new HashMap<String, Integer>(); // 用于log_clt_insert记数

	private int interval = 100;// 批量入库的间隔,默认100

	/**
	 * 解析入库中一个文件
	 * 
	 * @param file
	 *            原始文件路径
	 * @param taskInfo
	 *            任务信息
	 * @return 是否成功
	 */
	public boolean parse(String file, CollectObjInfo taskInfo) {
		int tmpId = taskInfo.getParseTmpID();
		Document docTemplet = getTemplet(tmpId);
		if (docTemplet == null) {
			return false;
		}
		tc = TempletConfig.getInstance(docTemplet);
		if (tc == null) {
			return false;
		}

		interval = tc.interval;

		for (Table t : tc.tables) {
			count.put(t.tableName, -1);
		}

		taskId = taskInfo.getTaskID();
		omcid = taskInfo.getDevInfo().getOmcID();
		stampTime = taskInfo.getLastCollectTime();

		File f = new File(file);
		if (!f.exists()) {
			logger.error(taskId + "-文件不存在:" + file);
			logToDb();
			return false;
		}

		InputStream in = null;
		Reader r = null;
		BufferedReader br = null;
		try {
			in = new FileInputStream(f);
			r = new InputStreamReader(in);
			br = new BufferedReader(r);
			String line = br.readLine();

			// 取bscid ,即MELID
			if (line != null) {
				try {
					line = line.trim().replace("SET MEL:", "").replace(";", "");
					String[] items = line.split(",");
					for (String s : items) {
						s = s.trim();
						if (s.startsWith("MELID=")) {
							bscId = Integer.parseInt(s.replace("MELID=", ""));
						}
					}
				} catch (Exception e) {
					logger.error("获取bsc_id时异常,line=" + line, e);
					bscId = -1;
				}
			}

			while ((line = br.readLine()) != null) {
				line = line.trim();
				if (line.equals("")) {
					continue;
				}
				Table t = getTableByLine(line);
				if (t == null) {
					continue;
				}

				for (String s : t.startSign) {
					line = line.replace(s, "");
				}
				line = line.substring(0, line.lastIndexOf(t.endSign));
				List<Record> records = null;
				if (tableNameToRecords.containsKey(t.tableName)) {
					records = tableNameToRecords.get(t.tableName);
				} else {
					records = new ArrayList<Record>();
					tableNameToRecords.put(t.tableName, records);
				}
				List<Field> fds = new ArrayList<Field>();
				String[] splited = line.split(t.splitSign);
				for (String s : splited) {
					String[] pair = s.split(t.nameValueSplitSign);
					String name = pair[0];
					String value = pair[1];
					Property p = getPropertyByPropName(name, t);
					if (p == null) {
						continue;
					} else {
						Field field = new Field(name.equals(t.key), p.columnName, value, p.dataType, p.dataFormat);
						if (!fds.contains(field)) {
							fds.add(field);
						}
						try {
							// 从name中，解析出每个表需要的几个特定属性
							if (name.equals("NAME")) {
								String[] tmp1 = value.split("/");
								for (String str : tmp1) {
									String[] tmp2 = str.split(":");
									Field ff = new Field(false, tmp2[0], tmp2[1], 1, null);
									if (!fds.contains(ff)) {
										fds.add(ff);
									}
								}
							}
						} catch (Exception e) {
							logger.error(e.getMessage() + ":" + value);
						}
					}
				}

				// 四个公共字段
				Field common1 = new Field(false, "STAMPTIME", Util.getDateString(stampTime), 3, "yyyy-mm-dd hh24:mi:ss");
				Field common2 = new Field(false, "COLLECTTIME", Util.getDateString(new Date()), 3, "yyyy-mm-dd hh24:mi:ss");
				Field common3 = new Field(false, "OMCID", String.valueOf(omcid), 1, null);
				Field common4 = new Field((t.key != null && t.key.equalsIgnoreCase("BSCID")), "BSCID", String.valueOf(bscId), 1, null);
				if (!fds.contains(common1)) {
					fds.add(common1);
				}
				if (!fds.contains(common2)) {
					fds.add(common2);
				}
				if (!fds.contains(common3)) {
					fds.add(common3);
				}
				if (!fds.contains(common4)) {
					fds.add(common4);
				}
				records.add(new Record(fds, t.tableName));
			}
			List<Record> records = mergeRecords();
			List<Record> tmp = new ArrayList<Record>();
			for (Record rx : records) {
				tmp.add(rx);
				insert(tmp, false);
			}
			insert(tmp, true);

			logToDb();
			return true;
		} catch (Exception e) {
			logger.error("解析时异常，文件：" + file, e);
			return false;
		} finally {
			tc = null;
			try {
				if (in != null) {
					in.close();
				}
				if (r != null) {
					r.close();
				}
				if (br != null) {
					br.close();
				}
			} catch (Exception e) {
			}
		}
	}

	private void logToDb() {
		Iterator<Entry<String, Integer>> it = count.entrySet().iterator();
		while (it.hasNext()) {
			Entry<String, Integer> en = it.next();
			dbLogger.log(omcid, en.getKey(), stampTime, en.getValue(), taskId);
		}
	}

	private void insert(List<Record> records, boolean now) {
		if (now || (records.size() % interval == 0)) {
			Statement st = null;
			Connection con = CommonDB.getConnection();

			try {
				con.setAutoCommit(false);
				st = con.createStatement();

				for (Record r : records) {
					if (r == null) {
						continue;
					}
					String sql = r.toInsert();
					if (Util.isNotNull(sql)) {
						st.addBatch(sql);
					}
				}
				st.executeBatch();
				con.commit();
				for (Record r : records) {
					if (r == null) {
						continue;
					}
					String tn = r.tableName;
					int c = count.get(tn);
					if (c == -1) {
						c = 0;
					}
					c++;
					count.put(tn, c);
				}
			} catch (Exception e) {
				logger.error("插入数据时异常", e);
			} finally {
				records.clear();
				try {
					if (st != null) {
						st.close();
					}
					if (con != null) {
						con.close();
					}
				} catch (Exception e) {
				}
			}
		}
	}

	/**
	 * 合并记录。 也就是将同一级别的数据中，name相同的记录，合并为一条
	 * 
	 * @return
	 */
	private List<Record> mergeRecords() {
		List<Record> ret = new ArrayList<Record>();
		Iterator<Entry<String, List<Record>>> it = tableNameToRecords.entrySet().iterator();
		Map<String, List<Record>> keyValueToRecords = new HashMap<String, List<Record>>();
		while (it.hasNext()) {
			Entry<String, List<Record>> e = it.next();
			// 以下三个表中，需要从CELLGLID中解析出LAC与CI
			if (e.getKey().contains("_BTS") || e.getKey().contains("_TGTBTS") || e.getKey().contains("_TGTFDD")) {
				for (Record r : e.getValue()) {
					if (r.fields.contains(new Field(false, "CELLGLID", null, 1, null))) {
						try {
							Field cellF = null;
							for (Field f : r.fields) {
								if (f.name.equals("CELLGLID")) {
									cellF = f;
								}
							}
							if (cellF != null && Util.isNotNull(cellF.value)) {
								String[] items = cellF.value.split("-");
								int lac = Integer.parseInt(items[2]);
								int ci = Integer.parseInt(items[3]);
								Field fLac = new Field(false, "LAC", String.valueOf(lac), 1, null);
								Field fCi = new Field(false, "CI", String.valueOf(ci), 1, null);
								if (!r.fields.contains(fLac)) {
									r.fields.add(fLac);
								}
								if (!r.fields.contains(fCi)) {
									r.fields.add(fCi);
								}
							}
						} catch (Exception ex) {
							logger.error(r + ":" + ex.getMessage());
						}
					}
				}
			}
			for (Record r : e.getValue()) {
				String keyValue = r.getKeyField() == null ? "*" : r.getKeyField().value;
				if (keyValueToRecords.containsKey(keyValue)) {
					keyValueToRecords.get(keyValue).add(r);
				} else {
					List<Record> rs = new ArrayList<Record>();
					rs.add(r);
					keyValueToRecords.put(keyValue, rs);
				}
			}
			Iterator<Entry<String, List<Record>>> subIt = keyValueToRecords.entrySet().iterator();
			Record tmpRecord = null;
			while (subIt.hasNext()) {
				Entry<String, List<Record>> en = subIt.next();
				String key = en.getKey();
				List<Record> rs = en.getValue();

				for (Record re : rs) {
					if (key.equals("*")) {
						ret.add(re);
					} else {
						if (tmpRecord == null) {
							tmpRecord = new Record(new ArrayList<Field>(), re.tableName);
						}
						List<Field> fs = re.fields;
						for (Field f : fs) {
							if (!tmpRecord.fields.contains(f)) {
								tmpRecord.fields.add(f);
							}
						}
					}
				}
				ret.add(tmpRecord);
				tmpRecord = null;
			}
			keyValueToRecords.clear();
		}

		tableNameToRecords.clear();
		tableNameToRecords = null;
		return ret;
	}

	private Property getPropertyByPropName(String PropName, Table t) {
		for (Property p : t.properties) {
			if (PropName.equals(p.propertyName)) {
				return p;
			}
		}

		return null;
	}

	private Table getTableByLine(String line) {
		List<Table> tables = tc.tables;
		for (Table t : tables) {
			List<String> startSigns = t.startSign;
			for (String s : startSigns) {
				if (line.startsWith(s)) {
					return t;
				}
			}
		}
		return null;
	}

	private Document getTemplet(int tmpId) {
		String sql = "select tempfilename from igp_conf_templet where tmpid = ?";
		Document doc = null;
		ResultSet rs = null;
		PreparedStatement st = null;
		Connection con = DbPool.getConn();
		try {
			st = con.prepareStatement(sql);
			st.setInt(1, tmpId);
			rs = st.executeQuery();
			if (rs.next()) {
				String fileName = rs.getString("TEMPFILENAME");
				if (Util.isNull(fileName)) {
					throw new Exception("模板文件名为空，SQL:" + sql);
				}
				File f = new File(SystemConfig.getInstance().getTempletPath(), fileName);
				if (!f.exists()) {
					throw new Exception("模板文件不存在，模板目录：" + SystemConfig.getInstance().getTempletPath() + "，模板文件名：" + fileName);
				}
				doc = new SAXReader().read(f);
			} else {
				throw new Exception("模板记录未找到，TMPID=" + tmpId);
			}
		} catch (Exception e) {
			logger.error("查找模板时异常:" + sql, e);
		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
				if (st != null) {
					st.close();
				}
				if (con != null) {
					con.close();
				}
			} catch (Exception e) {
			}
		}
		return doc;
	}

	class Record {

		List<Field> fields;

		String tableName;

		public Record(List<Field> fields, String tableName) {
			super();
			this.fields = fields;
			this.tableName = tableName;
		}

		public Field getKeyField() {
			for (Field f : fields) {
				if (f.isKey) {
					return f;
				}
			}
			return null;
		}

		public String toInsert() {
			StringBuilder sb = new StringBuilder();
			sb.append("insert into ").append(tableName).append(" (");
			for (Field f : fields) {
				sb.append(f.name).append(",");
			}
			if (sb.charAt(sb.length() - 1) == ',') {
				sb.deleteCharAt(sb.length() - 1);
			}
			sb.append(") values (");
			for (Field f : fields) {
				String val = null;
				switch (f.dataType) {
					case ConstDef.COLLECT_FIELD_DATATYPE_DIGITAL :
						val = f.value;
						break;
					case ConstDef.COLLECT_FIELD_DATATYPE_DATATIME :
						val = "to_date('" + f.value + "','" + f.dataFormat + "')";
						break;
					case ConstDef.COLLECT_FIELD_DATATYPE_STRING :
						val = "'" + f.value + "'";
						break;
					default :
						val = "'" + f.value + "'";
						break;
				}
				sb.append(val).append(",");
			}
			if (sb.charAt(sb.length() - 1) == ',') {
				sb.deleteCharAt(sb.length() - 1);
			}
			sb.append(")");
			return sb.toString();
		}

		@Override
		public boolean equals(Object obj) {
			Record r = (Record) obj;
			Field keyA = getKeyField();
			Field keyB = r.getKeyField();

			if (keyA == null || keyB == null) {
				return false;
			}

			return keyA.value.equals(keyB.value);
		}

		@Override
		public String toString() {
			StringBuffer sb = new StringBuffer();
			sb.append("[tableName:").append(tableName).append("][");
			for (Field f : fields) {
				sb.append(f.name).append(":").append(f.value).append(",");
			}
			return sb.toString();
		}
	}

	class Field {

		boolean isKey;

		String name;

		String value;

		int dataType;

		String dataFormat;

		public Field(boolean key, String name, String value, int dataType, String dataFormat) {
			super();
			this.isKey = key;
			this.name = name;
			this.value = value;
			this.dataType = dataType;
			this.dataFormat = dataFormat;
		}

		@Override
		public boolean equals(Object obj) {
			Field f = (Field) obj;
			return f.name.equals(name);
		}

	}

	public static void main(String[] args) throws Exception {
		CollectObjInfo c = new CollectObjInfo(554);
		c.setParseTmpID(713);
		c.setLastCollectTime(new Timestamp(Util.getDate1("2010-07-08 20:00:00").getTime()));
		DevInfo di = new DevInfo();
		di.setOmcID(98);
		c.setDevInfo(di);

		new SiemensCmParser().parse("C:\\Users\\ChenSijiang\\Desktop\\ebsc5.asc", c);
	}
}
