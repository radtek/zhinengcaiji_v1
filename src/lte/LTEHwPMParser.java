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
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import parser.Parser;
import parser.hw.pm.HuaweiPmParseException;
import task.CollectObjInfo;
import task.DevInfo;
import util.CommonDB;
import util.DbPool;
import util.LogMgr;
import util.Util;
import util.file.FileUtil;

/**
 * 华为FTP性能解析实现
 * 
 * @author yuy
 */
public class LTEHwPMParser extends Parser {

	private Timestamp stamptime;

	private String strStamptime;

	private Timestamp collecttime;

	private long mmeId;

	private long taskId;

	private String logKey = null;

	/* 整个文件的公共字段 */
	private Date strBeginTime;

	private String elementType;

	private String bsc;

	/* 单个表的公共字段 */
	private String measInfoId;

	private String granDuration;

	private String repDuration;

	private Date strEndTime;

	private static final Logger logger = LogMgr.getInstance().getSystemLogger();

	private static final String TERMINATED_FOR_SQLLDR = "|";
	
	private static final String columnNamePre = "COUNTER_";
	
	private static final String tableNamePre = "CLT_LTE_HW_PM_";
	
	private List<Map<String, String>> valsList = null;
	
	@Override
	public boolean parseData() {
		this.stamptime = collectObjInfo.getLastCollectTime();
		this.strStamptime = Util.getDateString(this.stamptime);
		this.collecttime = new Timestamp(System.currentTimeMillis());
		this.taskId = collectObjInfo.getTaskID();
		this.mmeId = collectObjInfo.getDevInfo().getOmcID();
		this.logKey = "[" + this.taskId + "][" + this.strStamptime + "]";
		try {
			parse();
		} catch (Exception e) {
			logger.error("解析数据时发生异常", e);
			return false;
		}
		return true;
	}

	public void parse() throws HuaweiPmParseException {

		InputStream in = null;
		XMLStreamReader reader = null;
		try {
			in = new FileInputStream(fileName);
			logger.debug(logKey + "开始解析 - " + fileName);
			reader = XMLInputFactory.newInstance().createXMLStreamReader(in);
			int type = -1;
			List<String> currTypes = new ArrayList<String>();
			List<String> currValues = new ArrayList<String>();
			String currTableName = null;
			String currLdn = null;
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
						if (tagName.equals("measResults")) {
							getCounterValues(currValues, reader.getElementText());
							if(valsList == null)
								valsList = new ArrayList<Map<String, String>>();
							Map<String, String> map = new HashMap<String, String>();
							for(int n = 0;n < currValues.size() && n < currTypes.size();n++){
								map.put(currTypes.get(n).toUpperCase(), currValues.get(n));
							}
							map.put("measObjLdn".toUpperCase(), currLdn);
							valsList.add(map);
						} else if (tagName.equals("measValue")) {
							currLdn = reader.getAttributeValue(null, "measObjLdn");
							if (currLdn != null)
								currLdn = currLdn.replace(TERMINATED_FOR_SQLLDR, " ");
						} else if (tagName.equals("measTypes")) {
							getCounterTypes(currTypes, reader.getElementText());
						} else if (tagName.equals("repPeriod"))
							this.repDuration = getNum(reader.getAttributeValue(null, "duration"));
						else if (tagName.equals("granPeriod")) {
							this.granDuration = getNum(reader.getAttributeValue(null, "duration"));
							this.strEndTime = strToDate(reader.getAttributeValue(null, "endTime"));
						} else if (tagName.equals("measInfo")) {
							this.measInfoId = reader.getAttributeValue(null, "measInfoId");
							currTableName = tableNamePre + this.measInfoId;
						} else if (tagName.equals("managedElement"))
							this.bsc = reader.getAttributeValue(null, "userLabel");
						else if (tagName.equals("measCollec") && this.strBeginTime == null)
							this.strBeginTime = strToDate(reader.getAttributeValue(null, "beginTime"));
						else if (tagName.equals("fileSender"))
							this.elementType = reader.getAttributeValue(null, "elementType");
						break;
					case XMLStreamConstants.END_ELEMENT :
						if(tagName.equalsIgnoreCase("measInfo")){
							if(valsList == null)
								continue;
							
							if (!DbUtil.tableMap.containsKey(currTableName)) {
								//如果map不存在，查找库是否存在该表
								if(!DbUtil.tableExists(currTableName, taskId)){
									tableHandler(currTypes, currTableName);
								}
							}
							
							List<String> columnList = DbUtil.tableMap.get(currTableName);
							
							//初始化sql 
							String insertSql = DbUtil.createInsertSql(currTableName, columnList);
							
							//输出
							exportBatch(insertSql, valsList, columnList);
							
							valsList.clear();
							valsList = null;
						}
						break;
					default :
						break;
				}
			}
			currTypes.clear();
			currTypes = null;
			currValues.clear();
			currValues = null;
		} catch (Exception e) {
			throw new HuaweiPmParseException(logKey + "解析华为性能文件时异常", e);
		} finally {
			try {
				if (reader != null)
					reader.close();
			} catch (Exception e) {
			}
			IOUtils.closeQuietly(in);
		}
	}

	/* 把"2010-02-03T10:30:00+08:00"这类格式的字符串转为日期 */
	private static Date strToDate(String time) throws Exception {
		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.YEAR, Integer.parseInt(time.substring(0, 4)));
		calendar.set(Calendar.MONTH, Integer.parseInt(time.substring(5, 7)) - 1);
		calendar.set(Calendar.DATE, Integer.parseInt(time.substring(8, 10)));
		calendar.set(Calendar.HOUR_OF_DAY, Integer.parseInt(time.substring(11, 13)));
		calendar.set(Calendar.MINUTE, Integer.parseInt(time.substring(14, 16)));
		calendar.set(Calendar.SECOND, 0);
		return calendar.getTime();
	}

	/* 从"PT1800S"这种字符串中取得中间的数字部分 */
	private static String getNum(String duration) throws Exception {
		return duration.substring(duration.indexOf("PT") + 2, duration.indexOf("S"));
	}

	private void getCounterTypes(List<String> target, String source) {
		target.clear();

		String[] items = source.split(" ");
		for (String s : items) {
			if (Util.isNotNull(s)) {
				target.add(columnNamePre +  s);
			}
		}
	}

	/* 从文本中解析出counter值 */
	private void getCounterValues(List<String> counterValues, String s) {
		counterValues.clear();

		String[] splited = s.split(" ");
		for (String item : splited) {
			if (Util.isNotNull(item)) {
				if (item.trim().equals("NIL")) {
					counterValues.add("");
				} else {
					counterValues.add(item);
				}
			}
		}

	}
	
	/**
	 * 表的处理
	 * @param record
	 * @param tn
	 * @throws SQLException
	 */
	public void tableHandler(List<String> list, String tn) throws SQLException {
		//不存在，先创建表
		Map<String, String> columnMap = new HashMap<String, String>();
		
		//公共字段
		columnMap.put("MMEID", "NUMBER");
		columnMap.put("STAMPTIME", "DATE");
		columnMap.put("COLLECTTIME", "DATE");
		columnMap.put("elementType", "VARCHAR2(50)");
		columnMap.put("beginTime", "DATE");
		columnMap.put("userLabel", "VARCHAR2(100)");
		columnMap.put("granPeriod_duration", "VARCHAR2(50)");
		columnMap.put("repPeriod_duration", "VARCHAR2(50)");
		columnMap.put("endTime", "DATE");
		
		for(String key : list){
			columnMap.put(key, "VARCHAR2(1000)");
		}
		
		columnMap.put("measObjLdn", "VARCHAR2(500)");
		
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
						statement.setLong(n++, mmeId);
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
					if(column.equalsIgnoreCase("elementType")){
						statement.setString(n++, elementType);
						continue;
					}
					if(column.equalsIgnoreCase("beginTime")){
						statement.setTimestamp(n++, new Timestamp(strBeginTime.getTime()));
						continue;
					}
					if(column.equalsIgnoreCase("userLabel")){
						statement.setString(n++, bsc);
						continue;
					}
					if(column.equalsIgnoreCase("granPeriod_duration")){
						statement.setString(n++, granDuration);
						continue;
					}
					if(column.equalsIgnoreCase("repPeriod_duration")){
						statement.setString(n++, repDuration);
						continue;
					}
					if(column.equalsIgnoreCase("endTime")){
						statement.setTimestamp(n++, new Timestamp(strEndTime.getTime()));
						continue;
					}
					
					statement.setString(n++, map.get(column.toUpperCase()));
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

	public static void main(String[] args) {
		CollectObjInfo info = new CollectObjInfo(1020110);
		DevInfo dev = new DevInfo();
		dev.setOmcID(1);
		info.setDevInfo(dev);
		info.setLastCollectTime(new Timestamp(11));
		try {
			String[] fileArray = FileUtil.listFile(new File("/home/yuy/my/requirement/LTE/lte_联通/0417/hwpm"));
			for(String file : fileArray){
				System.out.println("开始解析：" + file);
				LTEHwPMParser parser = new LTEHwPMParser();
				parser.setCollectObjInfo(info);
				parser.setFileName(file);
				parser.parseData();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("end");
	}
}
