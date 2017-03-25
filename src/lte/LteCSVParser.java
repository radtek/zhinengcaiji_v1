package lte;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import parser.Parser;
import sqlldr.SqlldrForLTEImpl;
import task.CollectObjInfo;
import task.DevInfo;
import templet.GenericSectionHeadD;
import templet.GenericSectionHeadP;
import util.Util;

import distributor.GenericSectionHeadDistributor;

/**
 * 带表头的按段解析
 * <p>
 * 适应于一个Ascii文件中多张表并且每张表都带表头的情况,能自动适应表头字段顺序的变化(包括字段新增、删除、顺便变化等) <br>
 * 典型应用:bell性能解析
 * <p>
 * 1.配置关注字段后数据源中字段的顺序可以进行调换而不影响数据采集;<br>
 * 2.数据区域段之间的顺序调换也不影响正常的采集;<br>
 * 3.模板中定义的ds处理完后程序即可退出,不再读取剩余的数据;
 * </p>
 * </p>
 * 
 * @author YangJian,Litp
 * @since 3.1
 * @see GenericSectionHeadP
 * @see GenericSectionHeadD
 * @see GenericSectionHeadDistributor
 */
public class LteCSVParser extends Parser {

	public String tableName;
	public long taskId;
	public int omcId;
	public Timestamp dateTime;
	
	public LteCSVParser() {
		super();
	}

	public LteCSVParser(CollectObjInfo obj) {
		this.collectObjInfo = obj;
	}

	@Override
	public boolean parseData() {
		if (Util.isNull(fileName))
			return false;
		collectObjInfo.spasOmcId = collectObjInfo.getDevInfo().getOmcID() + "";
		taskId = this.collectObjInfo.getTaskID();
		omcId = this.collectObjInfo.getDevInfo().getOmcID();
		dateTime = this.collectObjInfo.getLastCollectTime();
		String stamptime = Util.getDateString(dateTime);
		
		//获取表名
		String fullName = fileName.substring(fileName.lastIndexOf(File.separator) + 1);
		tableName = fullName.substring(fullName.indexOf("_") + 1, fullName.lastIndexOf("_"));
		
		//sqlldr初始化
		SqlldrForLTEImpl sqlldr = new SqlldrForLTEImpl(taskId, omcId, dateTime, "LTE");
		sqlldr.loadSingleTableStructrue(tableName);
		
		boolean flag = true;
		BufferedReader br = null;
		try {
			br = new BufferedReader(new InputStreamReader(new FileInputStream(fileName)));
			String line = null;
			String head = null;
			while ((line = br.readLine()) != null) {
				if(head == null){
					head = line;
					if(head.charAt(head.length() - 1) == ',')
						head = head.substring(0, head.length() - 1);
					head = dealValue(head);
					head += SqlldrForLTEImpl.splitStr + "MMEID" + SqlldrForLTEImpl.splitStr + "STAMPTIME" + SqlldrForLTEImpl.splitStr + "COLLECTTIME";
					head = head.replace(SqlldrForLTEImpl.splitStr, "`");
					List<String> colList = split(head, '`');
					
					//sqlldr文件初始化
					sqlldr.initSqlldr(tableName, colList);
					
					//处理表头
					head = head.replace("`", SqlldrForLTEImpl.splitStr);
					sqlldr.writeRows(head, tableName);
					continue;
				}
				String now = Util.getDateString(new Date());
				//处理内容行
				line = dealValue(line);
				line += SqlldrForLTEImpl.splitStr + omcId + SqlldrForLTEImpl.splitStr + stamptime + SqlldrForLTEImpl.splitStr + now;
				sqlldr.writeRows(line, tableName);
			}
			sqlldr.runSqlldr();
		} catch (FileNotFoundException e) {
			log.error(taskId + ": 解析失败,原因:文件不存在:" + this.fileName);
		} catch (Exception e) {
			log.error(taskId + ": 解析失败,原因：", e);
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
				}
			}
		}

		return flag;
	}

	/**
	 * 实现String.split(regex)方法
	 * 
	 * @param string
	 * @param split
	 * @return
	 */
	public static List<String> split(String string, char regex) {
		List<String> paraList = new ArrayList<String>();
		char[] str = string.toCharArray();
		int begin = 0;
		for (int i = 0; i < str.length; i++) {
			if (str[i] == regex) {
				paraList.add(string.substring(begin, i).trim());
				begin = i + 1;
			} else if (i + 1 == str.length) {
				String s = string.substring(begin, ++i);
				if (!"".equals(s.trim()))
					paraList.add(s.trim());
			}
		}
		return paraList;
	}

	/**
	 * 处理逗号、转义字符等
	 * @param value
	 * @return
	 */
	public static String dealValue(String value) {
		if(value == null)
			return null;
		value = value.trim().replaceAll(SqlldrForLTEImpl.splitStr, " ");
		value = value.replaceAll("\n", " ").replaceAll("\r", " ");
		
		String str = value;
		List<String> strList = new ArrayList<String>();
		StringBuffer sb = new StringBuffer();
		boolean flag = false;// 双引号标记
		char tmpChar = '0';
		int count = 0;
		for (char s : str.toCharArray()) {
			count++;
			if ((s == ',' || s == '，') && flag == false) {
				strList.add(sb.toString() + SqlldrForLTEImpl.splitStr);
				sb = new StringBuffer();
				tmpChar = s;
				continue;
			}
			if (s == '\"') {
				if(count == 1){
					flag = true;
					tmpChar = s;
					continue;
				}
				if (flag == true) {
					flag = false;
				} else if (tmpChar == ',' || tmpChar == '，') {
					flag = true;
				} else {
					sb.append(s);
				}
				tmpChar = s;
				continue;
			}
			sb.append(s);
			tmpChar = s;
		}
		if (sb.toString().length() > 0) {
			strList.add(sb.toString());
		}
		String strArray[] = strList.toArray(new String[strList.size()]);
		String strValue = "";
		for (String ss : strArray) {
			strValue += ss;
		}
		return strValue;
	}
	
	public static void main(String[] args) {
		CollectObjInfo obj = new CollectObjInfo(999);
		obj.setLastCollectTime(new Timestamp(new Date().getTime()));
		obj.setDevInfo(new DevInfo());

		LteCSVParser parser = new LteCSVParser(obj);
		parser.fileName = "F:\\yy\\igp_v1\\需求\\电信三期演示--LTE\\2013-10\\CM_CONFIGSET_F_V1_20131029.csv";
		parser.parseData();
	}
}
