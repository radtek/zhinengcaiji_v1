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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import task.CollectObjInfo;
import task.DevInfo;
import util.CommonDB;
import util.DbPool;
import util.LogMgr;
import util.Util;

/**
 * LTE MR数据解析入库。(hm,eric)
 * 
 * @author yuy 2014-03-21
 */
public class LTEMRParser {

	private String logKey;

	private long taskId;

	private Timestamp stamptime;

	private Timestamp collecttime;

	private int mmeId;
	
	private String eNBId;
	
	private String userLabel;
	
	private String mrName;
	
	private String keyNames;
	
	private List<Map<String, String>> objList;
	
	private List<String[]> valsList;

	private static final String TABLE_NAME_PREFIX = "CLT_MR_LTE_ERIC_";
//	private static final String TABLE_NAME_PREFIX = "CLT_MR_LTE_HW_";
	
	private static final Logger logger = LogMgr.getInstance().getSystemLogger();
	
	private static Map<String, String> specialColumnMap = new HashMap<String, String>();
	static{
		specialColumnMap.put("SctpAssociation", "VARCHAR2(100)");
	}

	public LTEMRParser(CollectObjInfo task) {
		this.taskId = task.getTaskID();
		this.stamptime = task.getLastCollectTime();
		this.collecttime = new Timestamp(System.currentTimeMillis());
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
			fac.setProperty("javax.xml.stream.supportDTD", false);
			fac.setProperty("javax.xml.stream.isValidating", false);
			reader = fac.createXMLStreamReader(in);

			/* type记录stax解析器每次读到的对象类型，是element，还是attribute等等…… */
			int type = -1;
			/* 保存当前的xml标签名 */
			String tagName = null;
			String lastTagName = null;
			Map<String, String> objMap = null;
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
						if (tagName.equals("eNB")) { /* 处理eNB标签 */
							eNBId = Util.nvl(reader.getAttributeValue(null, "id"), "");
							userLabel = Util.nvl(reader.getAttributeValue(null, "userLabel"), "");
						} else if (tagName.equals("measurement")) {/* 处理measurement标签 */
							mrName = Util.nvl(reader.getAttributeValue(null, "mrName"), "");
						} else if (tagName.equals("smr")) { /* 处理smr标签，获取字段名 */
							keyNames = Util.nvl(reader.getElementText(), "");
						} else if (tagName.equals("object")) {/* 处理object标签。 */
							String MmeCode = Util.nvl(reader.getAttributeValue(null, "MmeCode"), "");
							String MmeGroupId = Util.nvl(reader.getAttributeValue(null, "MmeGroupId"), "");
							String MmeUeS1apId = Util.nvl(reader.getAttributeValue(null, "MmeUeS1apId"), "");
							String TimeStamp = Util.nvl(reader.getAttributeValue(null, "TimeStamp"), "");
							String id = Util.nvl(reader.getAttributeValue(null, "id"), "");
							objMap = new HashMap<String, String>();
							objMap.put("MmeCode".toUpperCase(), MmeCode);
							objMap.put("MmeGroupId".toUpperCase(), MmeGroupId);
							objMap.put("MmeUeS1apId".toUpperCase(), MmeUeS1apId);
							objMap.put("TimeStamp".toUpperCase(), TimeStamp);
							objMap.put("id".toUpperCase(), id);
							if(objList == null){
								objList = new ArrayList<Map<String, String>>();
							}
							objList.add(objMap);
						} else if (tagName.equals("v")) { /* 处理v标签，获取字段值 */
							String vals = Util.nvl(reader.getElementText(), "");
							String[] valArray = Util.split(vals, " ");
							if(valsList == null){
								valsList = new ArrayList<String[]>();
							}
							valsList.add(valArray);
							//如果连续第二次是v标签
							if(tagName.equals(lastTagName))
								objList.add(objMap);
						}
						lastTagName = tagName;
						break;
					case XMLStreamConstants.END_ELEMENT :
						/* 遇到measurement结束标签 */
						if (tagName.equals("measurement")) {
							if(valsList == null)
								continue;
							keyNames = keyNames.replace(".", "_").toUpperCase();
							String[] keyArray = Util.split(keyNames, " ");
							
							//生成表并获取表名
							String tableName = getTableName(keyArray);

							List<String> columnList = DbUtil.tableMap.get(tableName);
							//初始化sql 
							String insertSql = DbUtil.createInsertSql(tableName, columnList);
							
							List<Map<String, String>> dataList = rebuildData(keyArray);
							
							//输出
							exportBatch(insertSql, dataList, columnList);
							
							//置空 释放
							mrName = null;
							keyNames = null;
							objList = null;
							valsList = null;
						}
						break;
					default :
						break;
				}
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
	 * 生成表名
	 * @param keyArray
	 * @return
	 * @throws SQLException
	 */
	public String getTableName(String[] keyArray) throws SQLException {
		String tableName;
		if("".equals(mrName)){
			String name = keyArray[0].replace("MR_", "");
			tableName = TABLE_NAME_PREFIX + name;
			if(tableName.length() > 30){
				int l = 30 - TABLE_NAME_PREFIX.length();
				tableName = TABLE_NAME_PREFIX + name.substring(name.length() - l);
			}
		}else{
			mrName = mrName.replace("MR.", "").replace(".", "_");
			tableName = TABLE_NAME_PREFIX + mrName;
			if(tableName.length() > 30){
				int l = 30 - TABLE_NAME_PREFIX.length();
				tableName = TABLE_NAME_PREFIX + mrName.substring(mrName.length() - l);
			}
		}
		
		if (!DbUtil.tableMap.containsKey(tableName)) {
			//如果map不存在，查找库是否存在该表
			if(!DbUtil.tableExists(tableName, taskId)){
				tableHandler(keyArray, tableName);
			}
		}
		return tableName;
	}
	
	/**
	 * 重建数据结构
	 * @param recordList
	 * @return
	 */
	public List<Map<String, String>> rebuildData(String[] keyArray) {
		List<Map<String, String>> list = new ArrayList<Map<String, String>>();
		for (int n = 0;n < valsList.size();n++) {
			Map<String, String> objectMap = objList.get(n);
			Map<String, String> dataMap = new HashMap<String, String>();
			dataMap.putAll(objectMap);
			
			String[] valsArray = valsList.get(n);
			for(int m = 0;m < keyArray.length && m < valsArray.length;m++){
				dataMap.put(keyArray[m], valsArray[m]);
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
	public void tableHandler(String[] keysArray, String tn) throws SQLException {
		//不存在，先创建表
		Map<String, String> columnMap = new HashMap<String, String>();
		//创建column
		
		//公共字段
		columnMap.put("MMEID", "NUMBER");
		columnMap.put("STAMPTIME", "DATE");
		columnMap.put("COLLECTTIME", "DATE");
		columnMap.put("eNBId", "VARCHAR2(100)");
		columnMap.put("userLabel", "VARCHAR2(100)");
		
		//obj标签
		Set<String> set = objList.get(0).keySet();
		for(String key : set){
			columnMap.put(key, "VARCHAR2(100)");
		}
		
		for(int n = 0;n < keysArray.length;n++){
			if(specialColumnMap.get(keysArray[n]) != null)
				columnMap.put(keysArray[n], specialColumnMap.get(keysArray[n]));
			else
				columnMap.put(keysArray[n], "VARCHAR2(100)");
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
		if (valsList == null || valsList.isEmpty())
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
					if(column.equalsIgnoreCase("eNBId")){
						statement.setString(n++, eNBId);
						continue;
					}
					if(column.equalsIgnoreCase("userLabel")){
						statement.setString(n++, userLabel);
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

	public static void main(String[] args) throws Exception {

		CollectObjInfo obj = new CollectObjInfo(755123);
		DevInfo dev = new DevInfo();
		dev.setOmcID(201);
		obj.setDevPort(21);
		obj.setDevInfo(dev);
//		obj.setLastCollectTime(new Timestamp(Util.getDate1("2013-11-14 9:15:00").getTime()));
		obj.setLastCollectTime(new Timestamp(Util.getDate1("2014-02-21 11:00:00").getTime()));

		LTEMRParser parser = new LTEMRParser(obj);
		//hw
//		File file = new File("/home/yuy/my/requirement/LTE/lte_联通/0320/华为/MR/TD-LTE_MRS_HUAWEI_711814_20140312151500.xml");
//		File file = new File("/home/yuy/my/requirement/LTE/lte_联通/0320/华为/MR/TD-LTE_MRO_HUAWEI_711814_20140312151500.xml");
		//eric
//		File file = new File("/home/yuy/my/requirement/LTE/lte_联通/0320/爱立信/TD-LTE_MRO_ERICSSON_OMC1_174207_20140221110000.xml");
		File file = new File("/home/yuy/my/requirement/LTE/lte_联通/0320/爱立信/TD-LTE_MRS_ERICSSON_OMC1_174207_20140221110000.xml");
		parser.parse(file);
	}
}
