package lte;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import parser.eric.pm.HelperForWCDMAEricssonPerformanceParser;
import task.CollectObjInfo;
import task.DevInfo;
import util.CommonDB;
import util.DbPool;
import util.LogMgr;
import util.Util;

/**
 * LTE爱立信性能数据解析入库。
 * 
 * @author yuy 2014-03-20
 */
public class LTEEricPMParser {

	private String logKey;

	private long taskId;

	private Timestamp stamptime;

	private Timestamp collecttime;

	private String strStamptime;

	private int mmeId;

	private String subnetworkRoot;

	private String subnetwork;

	private String meContext;

	private static final String TABLE_NAME_PREFIX = "CLT_PM_LTE_ERIC_";
	
	private Map<String, List<Record>> recordMap = null;
	
	private static final Logger logger = LogMgr.getInstance().getSystemLogger();
	
	private static Map<String, String> specialColumnMap = new HashMap<String, String>();
	static{
		specialColumnMap.put("SctpAssociation", "VARCHAR2(100)");
		specialColumnMap.put("pmHoExeInSuccQci", "VARCHAR2(800)");
		specialColumnMap.put("pmHoPrepAttLteIntraFQci", "VARCHAR2(800)");
		specialColumnMap.put("pmHoPrepAttLteInterFQci", "VARCHAR2(800)");
		specialColumnMap.put("pmHoPrepSuccLteInterFQci", "VARCHAR2(800)");
		specialColumnMap.put("pmHoPrepSuccLteIntraFQci", "VARCHAR2(800)");
		specialColumnMap.put("pmHoPrepOutAttQci", "VARCHAR2(800)");
		specialColumnMap.put("pmHoExeInAttQci", "VARCHAR2(800)");
		specialColumnMap.put("pmHoPrepOutSuccQci", "VARCHAR2(800)");
		specialColumnMap.put("EUtranCellRelation", "VARCHAR2(50)");
		specialColumnMap.put("pmErabModAttQci", "VARCHAR2(1000)");
		specialColumnMap.put("pmErabModSuccQci", "VARCHAR2(1000)");
	}

	public LTEEricPMParser(CollectObjInfo task) {
		this.taskId = task.getTaskID();
		this.stamptime = task.getLastCollectTime();
		this.collecttime = new Timestamp(System.currentTimeMillis());
		this.strStamptime = Util.getDateString(this.stamptime);
		this.logKey = String.format("[%s][%s]", this.taskId, this.strStamptime);
		this.mmeId = task.getDevInfo().getOmcID();
	}

	/**
	 * 开始解析一个原始文件。
	 * 
	 * @param fileName
	 */
	public void parse(File fd) {
		InputStream in = null;
		/* STAX解析器，解析xml原始文件 */
		XMLStreamReader reader = null;
		try {
			in = new FileInputStream(fd);

			/* 创建STAX解析器 */
			XMLInputFactory fac = XMLInputFactory.newInstance();
			fac.setProperty("javax.xml.stream.supportDTD", true);
			fac.setProperty("javax.xml.stream.isValidating", false);
			reader = fac.createXMLStreamReader(in);

			/* type记录stax解析器每次读到的对象类型，是element，还是attribute等等…… */
			int type = -1;
			/* 保存当前的xml标签名 */
			String tagName = null;
			/* 当前的moid */
			List<String[]> currMOID = null;
			/* 当前mt列表 */
			List<String> currMT = new ArrayList<String>();
			/* 当前r列表 */
			List<String> currR = new ArrayList<String>();
			/* 存储所有解析出的记录，以MOID作为key. */
			Map<MOID, Record> records = new HashMap<MOID, Record>();
			/* 开始迭代读取xml文件 */
			while (reader.hasNext()) {
				type = reader.next();
				/*
				 * 判断读取到的xml对象，只对START_ELEMENT和END_ELEMENT进行getLocalName()操作，在jdk1 .6023环境中，如果对CHARACTER进行getLocalName()操作，会抛异常。
				 */
				if (type == XMLStreamConstants.START_ELEMENT || type == XMLStreamConstants.END_ELEMENT)
					tagName = reader.getLocalName();
				if (tagName == null) {
					continue;
				}

				switch (type) {
					case XMLStreamConstants.START_ELEMENT :
						if (tagName.equals("r")) { /* 处理r标签，读取counter值 */
							currR.add(reader.getElementText());
						} else if (tagName.equals("mt")) {
							/* 处理mt标签，读取counter名 */
							currMT.add(reader.getElementText());
						} else if (tagName.equals("moid")) { /* 处理moid标签，读取counter类别、公共信息 */
							currMOID = HelperForWCDMAEricssonPerformanceParser.parseMOID(reader.getElementText());
						} else if (tagName.equals("sn")) {
							/* 处理sn标签，读取rnc名、nodeb名等信息。 */
							List<String[]> list = HelperForWCDMAEricssonPerformanceParser.parseSN(reader.getElementText());
							this.subnetworkRoot = HelperForWCDMAEricssonPerformanceParser.findByName(list, "SubNetworkRoot");
							this.subnetwork = HelperForWCDMAEricssonPerformanceParser.findByName(list, "SubNetwork");
							this.meContext = HelperForWCDMAEricssonPerformanceParser.findByName(list, "MeContext");
//							this.rncName = this.subnetwork;
//							this.isNodeB = (!this.subnetwork.equals(this.meContext));// SubNetwork和MeContext的值不一样的，表示解析的是一个NodeB级别的文件。
						}
						break;
					case XMLStreamConstants.END_ELEMENT :
						/* 遇到mv结束标签，应处理并清空r列表和当前moid */
						if (tagName.equals("mv")) {
							List<String> tmpR = new ArrayList<String>();
							tmpR.addAll(currR);
							List<String> tmpMT = new ArrayList<String>();
							tmpMT.addAll(currMT);
							MOID moid = new MOID(currMOID);
							Record record = new Record(moid, tmpMT, tmpR);
							//记录
							if(recordMap == null )
								recordMap = new HashMap<String, List<Record>>();
							if (records.containsKey(moid)){
								records.get(moid).appendRecord(record);
							}else{
								records.put(moid, record);
								//汇总
								if(recordMap.get(record.getTableName()) == null){
									List<Record> reList = new ArrayList<Record>();
									reList.add(records.get(moid));
									recordMap.put(record.getTableName(), reList);
								}else{
									recordMap.get(record.getTableName()).add(records.get(moid));
								}
							}
							currR.clear();
							currMOID = null;
						}
						/* 遇到mts结束标签，应处理并清空mt列表 */
						else if (tagName.equals("mts")) {
							currMT.clear();
						}
						break;
					default :
						break;
				}
			}
			//数据整理 输出
			currMOID = null;
			currMT = null;
			currR = null;
			tagName = null;
			Iterator<String> keyIt = recordMap.keySet().iterator();
			while (keyIt.hasNext()) {
				String tn = keyIt.next();
				List<Record> recordList = recordMap.get(tn);
				if (!DbUtil.tableMap.containsKey(tn)) {
					//如果map不存在，查找库是否存在该表
					if(!DbUtil.tableExists(tn, taskId)){
						tableHandler(recordList.get(0), tn);
					}
				}

				List<String> columnList = DbUtil.tableMap.get(tn);
				//初始化sql 
				String insertSql = DbUtil.createInsertSql(tn, columnList);
				//重新组装数据
				List<Map<String, String>> dataList = rebuildData(recordList);
				//输出
				exportBatch(insertSql, dataList, columnList);
			}

		} catch (Exception e) {
			logger.error(logKey + "解析时异常 - " + (fd != null ? fd.getAbsolutePath() : ""), e);
		} finally {
			try {
				reader.close();
			} catch (Exception exForReader) {
			}
			IOUtils.closeQuietly(in);
		}
	}

	/**
	 * 重建数据结构
	 * @param recordList
	 * @return
	 */
	public List<Map<String, String>> rebuildData(List<Record> recordList) {
		List<Map<String, String>> list = new ArrayList<Map<String, String>>();
		for (Record record : recordList) {
			Map<String, String> dataMap = new HashMap<String, String>();
			// moid
			List<String[]> moidList = record.moid.values;
			if (moidList != null && !moidList.isEmpty()) {
				for (String[] moid : moidList) {
					if (Util.isNotNull(moid[0])){
						String name = moid[0].toUpperCase();
						if(name.length() > 30){
							name = name.substring(name.length() - 30);
						}
						dataMap.put(name, moid[1]);
					}
				}
			}

			// 普通counter
			for (int i = 0; i < record.mtList.size(); i++) {
				String name = record.mtList.get(i);
				String colValue = record.rList.get(i);
				if (Util.isNotNull(name)){
					if(name.length() > 30){
						name = name.substring(name.length() - 30);
					}
					dataMap.put(name.toUpperCase(), colValue);
				}
			}
			list.add(dataMap);
		}
		return list;
	}

	/**
	 * 表的处理
	 * @param record
	 * @param tn
	 * @throws SQLException
	 */
	public void tableHandler(Record record, String tn) throws SQLException {
		//不存在，先创建表
		Map<String, String> columnMap = new HashMap<String, String>();
		//创建column
		List<String[]> values = record.moid.values;
		List<String> columnList = record.mtList;
		List<String> valueList = record.rList;
		
		//公共字段
		columnMap.put("MMEID", "NUMBER");
		columnMap.put("STAMPTIME", "DATE");
		columnMap.put("COLLECTTIME", "DATE");
		columnMap.put("SUBNETWORKROOT", "VARCHAR2(100)");
		columnMap.put("SUBNETWORK", "VARCHAR2(100)");
		columnMap.put("MECONTEXT", "VARCHAR2(100)");
		
		//moid
		for(String[] str : values){
			if(specialColumnMap.get(str[0]) != null)
				columnMap.put(str[0], specialColumnMap.get(str[0]));
			else
				columnMap.put(str[0], "VARCHAR2(" + str[1].length() * 10 + ")");
		}
		//mt
		for(int n = 0;n<columnList.size();n++){
			int len = 0;
			if(valueList.get(n) == null || valueList.get(n).length() == 0)
				len = 50;
			else
				len = valueList.get(n).length() * 10;
			
			if(specialColumnMap.get(columnList.get(n)) != null)
				columnMap.put(columnList.get(n), specialColumnMap.get(columnList.get(n)));
			else
				columnMap.put(columnList.get(n), "VARCHAR2(" + len + ")");
		}
		DbUtil.createTable(tn, columnMap);
		
		//存入缓存
		SortedMap<String, String> sortedMap = new TreeMap<String, String>(columnMap);//排序
		DbUtil.putMap(tn, sortedMap.keySet());
	}

	/**
	 * 将数据添加到批处理队列中
	 * 
	 * @param record
	 * @param statement
	 * @throws SQLException
	 * @throws ParseException
	 */
	private void exportBatch(String insertSql, List<Map<String, String>> dataList, List<String> columnList) throws SQLException {
		if (dataList == null || dataList.isEmpty())
			return;
		Connection con = null;
		con = DbPool.getConn();
		if (con == null) {
			logger.error("查找表是否存在:获取数据库连接失败！");
			return;
		}
		PreparedStatement statement = con.prepareStatement(insertSql);
		con.setAutoCommit(false);
		try {
			for(Map<String, String> map : dataList){
				int n = 1;
				for (String column : columnList) {
					if(column.equalsIgnoreCase("mmeId")){
						statement.setInt(n++, mmeId);
						continue;
					}
					if(column.equalsIgnoreCase("stamptime")){
						statement.setTimestamp(n++, stamptime);
						continue;
					}
					if(column.equalsIgnoreCase("collecttime")){
						statement.setTimestamp(n++, collecttime);
						continue;
					}
					if(column.equalsIgnoreCase("subnetworkRoot")){
						statement.setString(n++, subnetworkRoot);
						continue;
					}
					if(column.equalsIgnoreCase("subnetwork")){
						statement.setString(n++, subnetwork);
						continue;
					}
					if(column.equalsIgnoreCase("meContext")){
						statement.setString(n++, meContext);
						continue;
					}
					
					statement.setString(n++, map.get(column));
				}
				statement.addBatch();
			}
			statement.executeBatch();
			logger.info("入库成功" + dataList.size() + "条，sql=" + insertSql);
			con.commit();
		} catch (Exception e) {
			// 如果提交失败.则事务回滚,采用折半法查找异常的数据记录<br>
			con.rollback();
			CommonDB.close(null, statement, con);
			// 如果碰到失败一条，则成功条数-1，失败条数+1.
			logger.error("批量入库失败,sql=" + insertSql, e);
		} finally {
			CommonDB.close(null, statement, con);
		}
	}

	class MOID {

		List<String[]> values;

		int hash = -1;

		public MOID(List<String[]> values) {
			super();
			this.values = values;
		}

		@Override
		public int hashCode() {
			if (hash != -1)
				return hash;

			for (String[] array : values) {
				hash += array[0].hashCode();
				hash += array[1].hashCode();
			}

			return hash;
		}

		@Override
		public boolean equals(Object obj) {
			MOID m = (MOID) obj;
			if (m.values.size() != this.values.size())
				return false;
			for (int i = 0; i < this.values.size(); i++) {
				if (!HelperForWCDMAEricssonPerformanceParser.compareMOID(this.values, m.values))
					return false;
			}
			return true;
		}
	}

	/**
	 * 表示一条记录，包括的元素：MOID、mt列表、r列表。
	 * 
	 * @author ChenSijiang
	 */
	class Record {

		MOID moid;

		List<String> mtList;

		List<String> rList;

		String tableName;

		String type;

		public Record(MOID moid, List<String> mtList, List<String> rList) {
			super();
			this.moid = moid;
			this.mtList = mtList;
			this.rList = rList;
		}

		String findVal(String rawCounterName) {
			for (int i = 0; i < mtList.size(); i++) {
				String mt = mtList.get(i);
				if (mt.equalsIgnoreCase(rawCounterName)) {
					return rList.get(i);
				}
			}
			return null;
		}

		/* 获取moid中最后一个key的名字 */
		String getMOIDType() {
			if (this.type == null)
				this.type = HelperForWCDMAEricssonPerformanceParser.lastMOIDEntry(this.moid.values)[0];
			return this.type;
		}

		/* 获取表名 */
		String getTableName() {
			if (this.tableName == null) {
				this.tableName = TABLE_NAME_PREFIX + getMOIDType().toUpperCase();
				if (this.tableName.length() > 30)
					this.tableName = this.tableName.substring(0, 30);
			}
			return this.tableName;
		}

		/* 向本记录追加mt和r，即moid完全一样的情况下，是要合并在一起的，才是一条完整的记录。 */
		void appendRecord(Record record) {
			for (int i = 0; i < record.mtList.size(); i++) {
				this.mtList.add(record.mtList.get(i));
				this.rList.add(record.rList.get(i));
			}
		}

		/* 释放资源 */
		void dispose() {
			this.moid = null;
			if (this.mtList != null)
				this.mtList.clear();
			if (this.rList != null)
				this.rList.clear();
			this.mtList = null;
			this.rList = null;
		}

		@Override
		public String toString() {
			return "Record [moid=" + moid + ", mtList=" + mtList + ", rList=" + rList + ", tableName=" + tableName + ", type=" + type + "]";
		}
	}

	public static void main(String[] args) throws Exception {

		CollectObjInfo obj = new CollectObjInfo(755123);
		DevInfo dev = new DevInfo();
		dev.setOmcID(201);
		obj.setDevPort(21);
		obj.setDevInfo(dev);
		obj.setLastCollectTime(new Timestamp(Util.getDate1("2013-11-14 9:15:00").getTime()));

		LTEEricPMParser parser = new LTEEricPMParser(obj);
//		File file = new File("/home/yuy/my/requirement/LTE/lte_联通/0320/爱立信/TDD-ENB/201311191400=eNB012=XMLEE=A20131114.0100+0800-0115+0800_SubNetwork=ONRM_ROOT_MO,SubNetwork=ERBS,MeContext=eNB012_template.xml");
		File file = new File("/home/yuy/my/requirement/LTE/lte_联通/0320/爱立信/TDD-ENB/201311191400=eNB012=XMLOSS=A20131114.0100+0800-0115+0800_SubNetwork=ONRM_ROOT_MO,SubNetwork=ERBS,MeContext=eNB012_statsfile.xml");
		parser.parse(file);
	}
}
