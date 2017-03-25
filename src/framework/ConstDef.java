package framework;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.sql.Clob;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import util.LogMgr;
import util.Util;

/*
 *  {ConstDef}.java       
 *
 *  功能描述:   定义一些常用的定义、函数和枚举数据类型       
 *  作者:邓博文
 *  Email: dengbw@uway.cn
 *  创建日期:2007/12/12
 */
public class ConstDef {

	// 其他相关定义的定义
	public static final int MAX_RETRY_NUM = 5;// 数据采集时，最大的重试次数

	// 数据采集方式的定义
	public static final int COLLECT_TYPE_TELNET = 1; // TELNET采集方式

	public static final int COLLECT_TYPE_TCP = 2; // TCP采集方式

	public static final int COLLECT_TYPE_FTP = 3; // FTP采集方式

	public static final int COLLECT_TYPE_FILE = 4; // 文件采集方式

	public static final int COLLECT_TYPE_DataBase = 5; // 数据库采集方式Oracle,SQLSERVER,Informix

	// 虽然连接驱动不同，但是连接数据库方式都是一样
	public static final int COLLECT_TYPE_CORBA = 6; // Corba接口采集方式 by xumg

	public static final int COLLECT_TYPE_FTP_DOWNLOADER = 9;// FTP下载接口

	public static final String ESPECIAL_CHAR = "\""; // 特殊字符双赢号 by zengz

	public static final int PARSE_TYPE_SOCKET_M2000 = 8;// M2000北向告警字符流 by liuwx

	// 2010-03-02
	// /////////////////////////////////////////////////
	// 数据采集周期的定义
	public static final int COLLECT_PERIOD_FOREVER = 1; // 一直采集，直到程序停止

	public static final int COLLECT_PERIOD_DAY = 2; // 按天进行采集，一天采集一次

	public static final int COLLECT_PERIOD_HOUR = 3; // 按小时进行采集，一个小时采集一次

	public static final int COLLECT_PERIOD_MINUTE_HALFHOUR = 4; // 按半个小时周期进行采集

	public static final int COLLECT_PERIOD_MINUTE_QUARTER = 5; // 按一刻钟周期进行采集

	public static final int COLLECT_PERIOD_4HOUR = 6; // 按照4个小时采一次

	public static final int COLLECT_PERIOD_5MINUTE = 7; // 按照5分钟采一次

	public static final int COLLECT_PERIOD_HALFDAY = 8; // 按照12小时采一次

	public static final int COLLECT_PERIOD_WEEK = 9; // 按照一周采集一次

	public static final int COLLECT_PERIOD_MONTH = 10; // 按照一月采集一次 //add
														// 2011-10-17

	public static final int COLLECT_PERIOD_ONE_MINUTE = 11;// 每分钟执行 chensj
															// 2012-02-24

	public static final int COLLECT_PERIOD_10MINUTE = 12;// 每10分钟执行 yuy

	public static final int COLLECT_PERIOD_2MINUTE = 13;// 每2分钟执行 yuy

	// PCMD等数据定义为一直采集方式

	// 采集模板类型
	public static final int COLLECT_TEMPLATE_X = 999; // XParser 解析方式

	// (2010-01-08 chensj)
	public static final int COLLECT_TEMPLATE_NULL = 0;// 空解析模板，不做任何处理

	public static final int COLLECT_TEMPLATE_LINE = 1;// 按照行来分析

	public static final int COLLECT_TEMPLATE_SECT = 2;// 按照段来分析,FIELDITEM为table

	public static final int COLLECT_TEMPLATE_THRD = 3;// 用第三方工具分析 By xumg

	public static final int COLLECT_TEMPLATE_XML = 4;// 按XML文件解析 By xumg

	public static final int COLLECT_TEMPLATE_XLS = 5;// 按XLS文件解析 By xumg

	public static final int COLLECT_TEMPLATE_LUCENT_EVDO = 11; // 阿朗EVDO性能数据解析方式

	public static final int COLLECT_TEMPLATE_HUAWEI_FTP = 12; // 华为FTP性能文件 by

	// chensj
	// 2010.02.08
	public static final int COLLECT_TEMPLATE_PM_ZTE = 13; // 中兴性能 FTP by

	// chensj 2010.02.08
	public static final int COLLECT_TEMPLATE_HUAWEI_MML = 14; // 华为慢慢来数据 FTP

	// by litp
	// 2010.02.23
	public static final int COLLECT_TEMPLATE_HUAWEI_M2000 = 15; // M2000北向告警字符流

	// socket by
	// liuwx
	public static final int COLLECT_TEMPLATE_HUAWEI_CM_CSV = 16; // 华为无线参数数据(csv格式)

	// FTP by
	// yuanxf
	// 2010.3.12
	public static final int COLLECT_TEMPLATE_HUAWEI_CONFIG = 17; // 华为网元配置数据

	// FTP by
	// yuanxf
	// 2010.3.04
	public static final int COLLECT_TEMPLATE_HUAWEI_ALARM_SH = 2001; // 上海华为告警数据

	// FTP
	// by
	// yuanxf
	// 2010.04.09

	// 以下是联通使用的(start)
	public static final int COLLECT_TEMPLATE_ERIC_PM = 18; // 爱立信性能

	public static final int COLLECT_TEMPLATE_ERIC_CM = 19; // 爱立信参数

	public static final int COLLECT_TEMPLATE_ERIC_V1_CM = 23; // 爱立信一期参数

	public static final int COLLECT_TEMPLATE_HW_DBF = 35;

	// 联通使用(end)

	public static final int COLLECT_TEMPLATE_HUAWEI_AM = 20; // 华为告警 by

	// chensj
	// 2010.03.18
	public static final int COLLECT_TEMPLATE_SECT_21 = 21;// 按照段来分析,SECTITEM为table

	public static final int COLLECT_TEMPLATE_GPS_ENSURE_POS = 24;// add on

	// 2010-03-24

	/**
	 * WCDMA华为M2000告警数据采集，socket方式。
	 * */
	public static final int WCDMA_HW_M2000_ALARM_STREAM_PARSER = 2006;

	public static final int COLLECT_DATA_BUFF_SIZE = 1024 * 1024; // 采集数据字节大小

	// 日志类型
	public static final int COLLECT_LOGTYPE_NATURAL = 1;// 正常日志

	public static final int COLLECT_LOGTYPE_ERROR = 2;// 错误日志

	// 段的分隔类型
	public static final int COLLECT_SECT_SCANTYPE_N = 1; // 利用\n\n来分隔来分段

	public static final int COLLECT_SECT_SCANTYPE_KEYWORD = 2;// 利用段前段后关键字标记来分段

	public static final int COLLECT_SECT_SCANTYPE_SPLIT_KEYWORD = 3;// 利用段关键字标记来分行。

	// 根据关键字属于什么段类型，在此定义关键字查找方式
	public static final int COLLECT_SECT_KEYWORD_HEAD = 1; // 按段开始的一个关键字来分析

	public static final int COLLECT_SECT_KEYWORD_TAIL = 2; // 按段最后的一个关键字来分析

	public static final int COLLECT_SECT_KEYWORD_SENTER = 3;// 按段中间的一个关键字来分析

	// 按段解析中，字段解析类型
	public static final int COLLECT_SECT_PARSE_BITPOS = 1; // 按照开始位置与长度来解析

	public static final int COLLECT_SECT_PARSE_KEYWORD = 2; // 按照开始字符与结束字符来解析

	public static final int COLLECT_SECT_PARSE_TOEND = 3; // 类型2的特殊类型,截取位置从当前位置到最后一个字符

	public static final int COLLECT_SECT_PARSE_KEYFIELDONLYONE = 4; // 类型4的特殊类型,在段中，关键字是唯一的

	public static final int COLLECT_SECT_PARSE_COMPLEX = 10; // 复合类型(他的解析方式可能是1,2,3)

	public static final int COLLECT_SECT_PARSE_SPLIT = 11; // 将一个字段分割成多个字段

	public static final int COLLECT_SECT_PARSE_SPLIT_LINE = 12; // 将一个字段分割成多行

	// 按行解析中，字段解析类型
	public static final int COLLECT_LINE_PARSE_SPLIT = 1; // Split解析

	public static final int COLLECT_LINE_PARSE_BITPOS = 2; // 按照开始位置与长度来解析

	public static final int COLLECT_LINE_PARSE_RAW = 3; // 不解析直接发布

	public static final int COLLECT_LINE_PARSE_FREEDOM = 4; // 保存原始状态的类型

	// 字段的数据类型
	public static final int COLLECT_FIELD_DATATYPE_DIGITAL = 1;// 数字类型

	public static final int COLLECT_FIELD_DATATYPE_STRING = 2;// 字符型

	public static final int COLLECT_FIELD_DATATYPE_DATATIME = 3;// 时间类型

	public static final int COLLECT_FIELD_DATATYPE_LOB = 4;// 无限长度

	public static final int COLLECT_FIELD_TO_NUMBER = 5;// 进行to_number()转换

	// liangww add 2012-06-06 用于sqlldr时的字段特殊处理类型
	public static final int COLLECT_FIELD_SPECIAL_FORMAT = 6;// 特殊format类型，
	
	public static final int COLLECT_FIELD_DATATYPE_DATATIME2 = 7;// 英文时间类型

	/* 入库方式 */
	public static final int COLLECT_DISTRIBUTE_INSERT = 1;// insert方式

	public static final int COLLECT_DISTRIBUTE_SQLLDR = 2;// sqlldr方式

	public static final int COLLECT_DISTRIBUTE_SQLLDR_DYNAMIC = 3;// sqlldr方式,采用动态列导入

	public static final int COLLECT_DISTRIBUTE_FILE = 4;

	/* MR采集源,定位算法使用 */
	public static final int MR_SOURCE_ZCTT = 0; // 中创硬采

	public static final int MR_SOURCE_ZTE1 = 1; // 中兴硬采

	public static final int MR_SOURCE_ZTE2 = 2; // 中兴硬采，中兴硬采IMSI、TMSI、session版

	public static final int MR_SOURCE_MOTO = 3; // 摩托

	/** 取（）之间的内容 */
	public static String getExpression(String str) {

		String result = null;

		int b = str.indexOf("(");
		int e = str.indexOf(")");

		if (b > 0 && e > b) {
			result = str.substring(b + 1, e);
		}

		if (result != null && !result.contains("%%"))
			return null;

		return result;
	}

	public static String ParseFilePath(String strPath, Timestamp timestamp) {
		// 如：zblecp-2006102311.hsmr 文件名包含时间，表示这个时间的数据。
		// 这个时间有是timestamp传递进来的时间
		// 但是，有的文件名是一个时间段，前后有2个时间。
		// 如：Domain125_PSbasicmeasurement_18Jul2008_0900-18Jul2008_1000.csv
		// 现在从strPath传递进来一个参数表示前后时间的间隔数，
		// 如：Domain125_PSbasicmeasurement_%%D%%EM%%Y_%%H%%m-%%ND%%ENM%%NY_%%NH%%Nm.csv|360000

		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");

		int iHwrIdx = strPath.indexOf("%%TA");
		int iDiff = 0;
		if (iHwrIdx >= 0) {
			// 偏移量 0－9，只支持正偏移
			iDiff = Integer.parseInt(strPath.substring(iHwrIdx + 4, iHwrIdx + 5));
			strPath = strPath.replaceAll("%%TA.", "");
		}

		timestamp = new Timestamp(timestamp.getTime() + iDiff * 3600 * 1000);
		String strTime = formatter.format(timestamp);

		if (strPath.indexOf("%%Y") >= 0)
			strPath = strPath.replace("%%Y", strTime.substring(0, 4));
		Calendar calendar = Calendar.getInstance();

		Date date = new Date();
		date.setTime(timestamp.getTime());
		calendar.setTime(new Date());

		calendar.setTime(date);
		int nDayOrYear = calendar.get(Calendar.DAY_OF_YEAR);

		if (strPath.indexOf("%%WEEK") >= 0) {
			int dow = calendar.get(Calendar.DAY_OF_WEEK);
			dow = dow - 1;
			if (dow == 0)
				dow = 7;
			strPath = strPath.replace("%%WEEK", String.valueOf(dow));
		}

		if (nDayOrYear < 10)
			strPath = strPath.replace("%%DayOfYear", "00" + nDayOrYear);
		else if (nDayOrYear < 100)
			strPath = strPath.replace("%%DayOfYear", "0" + nDayOrYear);
		else
			strPath = strPath.replace("%%DayOfYear", String.valueOf(nDayOrYear));

		if (strPath.indexOf("%%y") >= 0)
			strPath = strPath.replace("%%y", strTime.substring(2, 4));

		if (strPath.indexOf("%%EM") >= 0) {
			switch (Integer.parseInt(strTime.substring(4, 6))) {
				case 1 :
					strPath = strPath.replace("%%EM", "Jan");
					break;
				case 2 :
					strPath = strPath.replace("%%EM", "Feb");
					break;
				case 3 :
					strPath = strPath.replace("%%EM", "Mar");
					break;
				case 4 :
					strPath = strPath.replace("%%EM", "Apr");
					break;
				case 5 :
					strPath = strPath.replace("%%EM", "May");
					break;
				case 6 :
					strPath = strPath.replace("%%EM", "Jun");
					break;
				case 7 :
					strPath = strPath.replace("%%EM", "Jul");
					break;
				case 8 :
					strPath = strPath.replace("%%EM", "Aug");
					break;
				case 9 :
					strPath = strPath.replace("%%EM", "Sep");
					break;
				case 10 :
					strPath = strPath.replace("%%EM", "Oct");
					break;
				case 11 :
					strPath = strPath.replace("%%EM", "Nov");
					break;
				case 12 :
					strPath = strPath.replace("%%EM", "Dec");
					break;
			}
		}

		if (strPath.indexOf("%%M") >= 0)
			strPath = strPath.replace("%%M", strTime.substring(4, 6));

		if (strPath.indexOf("%%d") >= 0)
			strPath = strPath.replace("%%d", strTime.substring(6, 8));

		if (strPath.indexOf("%%D") >= 0)
			strPath = strPath.replace("%%D", strTime.substring(6, 8));

		// add by liuwx 针对天,小时是否补0 ,如果天为0-9 ，则返回0-9 天为10-31返回10-31 ，小时0-9则0-9
		// ，小时10-23则10-23
		if (strPath.indexOf("%%fd") >= 0) {
			strPath = strPath.replace("%%fd", strTime.substring(6, 8));
		}
		String sd = null;
		if (strPath.indexOf("%%FD") >= 0) {
			sd = strTime.substring(6, 8);
			try {
				if (Integer.valueOf(sd) < 10) {
					strPath = strPath.replace("%%FD", strTime.substring(7, 8));
				} else
					strPath = strPath.replace("%%FD", strTime.substring(6, 8));
			} catch (NumberFormatException e) {
				e.printStackTrace();
			}
		}
		String fh = null;
		if (strPath.indexOf("%%FH") >= 0) {
			String strHour = getExpression(strPath);
			if (strHour != null && !strHour.equals("")) {
				String strHourTmp = strHour.replaceAll("%%FH", strTime.substring(8, 10));
				int nHour = Util.parseExpression(strHourTmp);

				/*
				 * ChenSijiang 2011-01-18 如果现在是23点，那么(%%H+1)会变成24，应该是不存在24点的说法。所以此处改成，如果小时过了23之后，变为0.
				 */
				if (nHour > 23)
					nHour = 0;

				strPath = strPath.replace("(" + strHour + ")", String.valueOf(nHour));
				if (nHour < 10)
					strPath = strPath.replaceAll("%%FH", strTime.substring(7, 10));
				else
					strPath = strPath.replaceAll("%%FH", strTime.substring(8, 10));
			} else {
				fh = strTime.substring(8, 10);
				if (Integer.valueOf(fh) < 10) {
					strPath = strPath.replace("%%FH", strTime.substring(9, 10));
				} else
					strPath = strPath.replace("%%FH", strTime.substring(8, 10));
			}
		}
		// end add

		// G网贝尔用，有24点的文件名。
		if (strPath.indexOf("%%BH") >= 0) {
			String strHour = getExpression(strPath);
			if (strHour != null && !strHour.equals("")) {
				String strHourTmp = strHour.replaceAll("%%BH", strTime.substring(8, 10));
				int nHour = Util.parseExpression(strHourTmp);

				strPath = strPath.replace("(" + strHour + ")", Util.trimHour(nHour));
				strPath = strPath.replaceAll("%%BH", strTime.substring(8, 10));
			} else {
				strPath = strPath.replaceAll("%%BH", strTime.substring(8, 10));
			}
		}

		if (strPath.indexOf("%%H") >= 0) {
			String strHour = getExpression(strPath);
			if (strHour != null && !strHour.equals("")) {
				String strHourTmp = strHour.replaceAll("%%H", strTime.substring(8, 10));
				int nHour = Util.parseExpression(strHourTmp);

				/*
				 * ChenSijiang 2011-01-18 如果现在是23点，那么(%%H+1)会变成24，应该是不存在24点的说法。所以此处改成，如果小时过了23之后，变为0.
				 */
				if (nHour > 23 && !strPath.toLowerCase().contains("/apme/obsynt/"))
					nHour = 0;

				// String temp = strPath.substring(strPath.indexOf("("),
				// strPath.indexOf(")") + 1);

				// if ( temp.contains("%%") )
				strPath = strPath.replace("(" + strHour + ")", Util.trimHour(nHour));
				strPath = strPath.replaceAll("%%H", strTime.substring(8, 10));
			} else {
				strPath = strPath.replaceAll("%%H", strTime.substring(8, 10));
			}
		}

		if (strPath.indexOf("%%h") >= 0)
			strPath = strPath.replaceAll("%%h", strTime.substring(8, 10));

		if (strPath.indexOf("%%m") >= 0)
			strPath = strPath.replace("%%m", strTime.substring(10, 12));

		if (strPath.indexOf("%%s") >= 0)
			strPath = strPath.replace("%%s", strTime.substring(12, 14));

		if (strPath.indexOf("%%S") >= 0)
			strPath = strPath.replace("%%S", strTime.substring(12, 14));

		String strInterval = "";
		int nInterval = 0;
		if (strPath.indexOf("|") > 0) {
			strInterval = strPath.substring(strPath.indexOf("|") + 1);
			strPath = strPath.substring(0, strPath.indexOf("|"));

			nInterval = Integer.parseInt(strInterval);
			timestamp = new Timestamp(timestamp.getTime() + nInterval);
			strTime = formatter.format(timestamp);

			calendar.setTime(timestamp);

			if (strPath.indexOf("%%NWEEK") >= 0) {
				int dow = calendar.get(Calendar.DAY_OF_WEEK);
				dow = dow - 1;
				if (dow == 0)
					dow = 7;
				strPath = strPath.replace("%%NWEEK", String.valueOf(dow));
			}

			if (strPath.indexOf("%%NY") >= 0)
				strPath = strPath.replace("%%NY", strTime.substring(0, 4));

			if (strPath.indexOf("%%Ny") >= 0)
				strPath = strPath.replace("%%Ny", strTime.substring(2, 4));

			if (strPath.indexOf("%%NEM") >= 0) {
				switch (Integer.parseInt(strTime.substring(4, 6))) {
					case 1 :
						strPath = strPath.replace("%%NEM", "Jan");
						break;
					case 2 :
						strPath = strPath.replace("%%NEM", "Feb");
						break;
					case 3 :
						strPath = strPath.replace("%%NEM", "Mar");
						break;
					case 4 :
						strPath = strPath.replace("%%NEM", "Apr");
						break;
					case 5 :
						strPath = strPath.replace("%%NEM", "May");
						break;
					case 6 :
						strPath = strPath.replace("%%NEM", "Jun");
						break;
					case 7 :
						strPath = strPath.replace("%%NEM", "Jul");
						break;
					case 8 :
						strPath = strPath.replace("%%NEM", "Aug");
						break;
					case 9 :
						strPath = strPath.replace("%%NEM", "Sep");
						break;
					case 10 :
						strPath = strPath.replace("%%NEM", "Oct");
						break;
					case 11 :
						strPath = strPath.replace("%%NEM", "Nov");
						break;
					case 12 :
						strPath = strPath.replace("%%NEM", "Dec");
						break;
				}
			}

			if (strPath.indexOf("%%NM") >= 0)
				strPath = strPath.replace("%%NM", strTime.substring(4, 6));

			if (strPath.indexOf("%%Nd") >= 0)
				strPath = strPath.replace("%%Nd", strTime.substring(6, 8));

			if (strPath.indexOf("%%ND") >= 0)
				strPath = strPath.replace("%%ND", strTime.substring(6, 8));

			if (strPath.indexOf("%%NH") >= 0)
				strPath = strPath.replace("%%NH", strTime.substring(8, 10));

			if (strPath.indexOf("%%NV4") >= 0) {
				int nNum = Integer.parseInt(strTime.substring(8, 10));
				nNum = (nNum + 1) / 4;
				strPath = strPath.replace("%%NV4", "0" + nNum);
			}

			if (strPath.indexOf("%%Nh") >= 0)
				strPath = strPath.replace("%%Nh", strTime.substring(8, 10));

			if (strPath.indexOf("%%Nm") >= 0)
				strPath = strPath.replace("%%Nm", strTime.substring(10, 12));

			if (strPath.indexOf("%%Ns") >= 0)
				strPath = strPath.replace("%%Ns", strTime.substring(12, 14));

			if (strPath.indexOf("%%NS") >= 0)
				strPath = strPath.replace("%%NS", strTime.substring(12, 14));
		}

		return strPath;
	}

	public static String ParseFilePathForDB(String strPath, Timestamp timestamp) {
		// 如：zblecp-2006102311.hsmr 文件名包含时间，表示这个时间的数据。
		// 这个时间有是timestamp传递进来的时间
		// 但是，有的文件名是一个时间段，前后有2个时间。
		// 如：Domain125_PSbasicmeasurement_18Jul2008_0900-18Jul2008_1000.csv
		// 现在从strPath传递进来一个参数表示前后时间的间隔数，
		// 如：Domain125_PSbasicmeasurement_%%D%%EM%%Y_%%H%%m-%%ND%%ENM%%NY_%%NH%%Nm.csv|360000

		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");

		int iHwrIdx = strPath.indexOf("%%TA");
		int iDiff = 0;
		if (iHwrIdx >= 0) {
			// 偏移量 0－9，只支持正偏移
			iDiff = Integer.parseInt(strPath.substring(iHwrIdx + 4, iHwrIdx + 5));
			strPath = strPath.replaceAll("%%TA.", "");
		}

		timestamp = new Timestamp(timestamp.getTime() + iDiff * 3600 * 1000);
		String strTime = formatter.format(timestamp);

		if (strPath.indexOf("%%Y") >= 0)
			strPath = strPath.replace("%%Y", strTime.substring(0, 4));
		Calendar calendar = Calendar.getInstance();

		Date date = new Date();
		date.setTime(timestamp.getTime());
		calendar.setTime(new Date());

		calendar.setTime(date);
		int nDayOrYear = calendar.get(Calendar.DAY_OF_YEAR);

		if (nDayOrYear < 10)
			strPath = strPath.replace("%%DayOfYear", "00" + nDayOrYear);
		else if (nDayOrYear < 100)
			strPath = strPath.replace("%%DayOfYear", "0" + nDayOrYear);
		else
			strPath = strPath.replace("%%DayOfYear", String.valueOf(nDayOrYear));

		if (strPath.indexOf("%%y") >= 0)
			strPath = strPath.replace("%%y", strTime.substring(2, 4));

		if (strPath.indexOf("%%EM") >= 0) {
			switch (Integer.parseInt(strTime.substring(4, 6))) {
				case 1 :
					strPath = strPath.replace("%%EM", "Jan");
					break;
				case 2 :
					strPath = strPath.replace("%%EM", "Feb");
					break;
				case 3 :
					strPath = strPath.replace("%%EM", "Mar");
					break;
				case 4 :
					strPath = strPath.replace("%%EM", "Apr");
					break;
				case 5 :
					strPath = strPath.replace("%%EM", "May");
					break;
				case 6 :
					strPath = strPath.replace("%%EM", "Jun");
					break;
				case 7 :
					strPath = strPath.replace("%%EM", "Jul");
					break;
				case 8 :
					strPath = strPath.replace("%%EM", "Aug");
					break;
				case 9 :
					strPath = strPath.replace("%%EM", "Sep");
					break;
				case 10 :
					strPath = strPath.replace("%%EM", "Oct");
					break;
				case 11 :
					strPath = strPath.replace("%%EM", "Nov");
					break;
				case 12 :
					strPath = strPath.replace("%%EM", "Dec");
					break;
			}
		}

		if (strPath.indexOf("%%M") >= 0)
			strPath = strPath.replace("%%M", strTime.substring(4, 6));

		if (strPath.indexOf("%%NZM") >= 0) {
			// 前面未补"0"的月份(NZM=no zero month) chensj 2011-7-18增加
			strPath = strPath.replace("%%NZM", String.valueOf(Integer.parseInt(strTime.substring(4, 6))));
		}

		if (strPath.indexOf("%%d") >= 0)
			strPath = strPath.replace("%%d", strTime.substring(6, 8));

		if (strPath.indexOf("%%D") >= 0)
			strPath = strPath.replace("%%D", strTime.substring(6, 8));

		if (strPath.indexOf("%%H") >= 0) {
			strPath = strPath.replace("%%H", strTime.substring(8, 10));
		}

		if (strPath.indexOf("%%h") >= 0)
			strPath = strPath.replace("%%h", strTime.substring(8, 10));

		if (strPath.indexOf("%%m") >= 0)
			strPath = strPath.replace("%%m", strTime.substring(10, 12));

		if (strPath.indexOf("%%s") >= 0)
			strPath = strPath.replace("%%s", strTime.substring(12, 14));

		if (strPath.indexOf("%%S") >= 0)
			strPath = strPath.replace("%%S", strTime.substring(12, 14));

		String strInterval = "";
		int nInterval = 0;
		if (strPath.indexOf("|") > 0) {
			strInterval = strPath.substring(strPath.indexOf("|") + 1);
			strPath = strPath.substring(0, strPath.indexOf("|"));

			nInterval = Integer.parseInt(strInterval);
			timestamp = new Timestamp(timestamp.getTime() + nInterval);
			strTime = formatter.format(timestamp);

			if (strPath.indexOf("%%NY") >= 0)
				strPath = strPath.replace("%%NY", strTime.substring(0, 4));

			if (strPath.indexOf("%%Ny") >= 0)
				strPath = strPath.replace("%%Ny", strTime.substring(2, 4));

			if (strPath.indexOf("%%NEM") >= 0) {
				switch (Integer.parseInt(strTime.substring(4, 6))) {
					case 1 :
						strPath = strPath.replace("%%NEM", "Jan");
						break;
					case 2 :
						strPath = strPath.replace("%%NEM", "Feb");
						break;
					case 3 :
						strPath = strPath.replace("%%NEM", "Mar");
						break;
					case 4 :
						strPath = strPath.replace("%%NEM", "Apr");
						break;
					case 5 :
						strPath = strPath.replace("%%NEM", "May");
						break;
					case 6 :
						strPath = strPath.replace("%%NEM", "Jun");
						break;
					case 7 :
						strPath = strPath.replace("%%NEM", "Jul");
						break;
					case 8 :
						strPath = strPath.replace("%%NEM", "Aug");
						break;
					case 9 :
						strPath = strPath.replace("%%NEM", "Sep");
						break;
					case 10 :
						strPath = strPath.replace("%%NEM", "Oct");
						break;
					case 11 :
						strPath = strPath.replace("%%NEM", "Nov");
						break;
					case 12 :
						strPath = strPath.replace("%%NEM", "Dec");
						break;
				}
			}

			if (strPath.indexOf("%%NM") >= 0)
				strPath = strPath.replace("%%NM", strTime.substring(4, 6));

			if (strPath.indexOf("%%Nd") >= 0)
				strPath = strPath.replace("%%Nd", strTime.substring(6, 8));

			if (strPath.indexOf("%%ND") >= 0)
				strPath = strPath.replace("%%ND", strTime.substring(6, 8));

			if (strPath.indexOf("%%NH") >= 0)
				strPath = strPath.replace("%%NH", strTime.substring(8, 10));

			if (strPath.indexOf("%%NV4") >= 0) {
				int nNum = Integer.parseInt(strTime.substring(8, 10));
				nNum = (nNum + 1) / 4;
				strPath = strPath.replace("%%NV4", "0" + nNum);
			}

			if (strPath.indexOf("%%Nh") >= 0)
				strPath = strPath.replace("%%Nh", strTime.substring(8, 10));

			if (strPath.indexOf("%%Nm") >= 0)
				strPath = strPath.replace("%%Nm", strTime.substring(10, 12));

			if (strPath.indexOf("%%Ns") >= 0)
				strPath = strPath.replace("%%Ns", strTime.substring(12, 14));

			if (strPath.indexOf("%%NS") >= 0)
				strPath = strPath.replace("%%NS", strTime.substring(12, 14));
		}

		return strPath;
	}

	/* 在Oracle 数据库中将Clob转换成String类型 */
	public static String ClobParse(Clob clob) {
		if (clob == null) {
			return "";
		}
		StringBuilder sb = new StringBuilder();
		try {
			java.io.Reader is = clob.getCharacterStream();
			java.io.BufferedReader br = new java.io.BufferedReader(is);
			String s = null;
			// 当到达最后一行readLine的时候,s为空退出
			while ((s = br.readLine()) != null) {
				sb.append(s);
				// content += s;
			}
			br.close();
			is.close();
			// content = clob.getSubString((long)1,(int)clob.length());
		} catch (Exception e) {
			LogMgr.getInstance().getSystemLogger().error("读取clob字段时发生异常。", e);
		}
		return sb.toString();
	}

	public static String CreateFolder(String strCurrentPath, long TaskID, String strFileName) {
		/*
		 * /w/zte/!20110209{888}!/00.txt
		 */

		String fn = strFileName;

		if (Util.isNotNull(strFileName)) {
			if (strFileName.contains("!") && strFileName.contains("{") && strFileName.contains("}")) {
				int begin = strFileName.indexOf("!");
				int end = strFileName.lastIndexOf("!");
				if (begin > -1 && end > -1 && begin < end) {
					String content = strFileName.substring(begin, end + 1);
					int cBegin = content.indexOf("{");
					int cEnd = content.indexOf("}");
					if (cBegin > -1 && cEnd > -1 && cBegin < cEnd) {
						String dir = content.substring(cBegin + 1, cEnd);
						fn = fn.replace(content, dir);
					}
				}
			}
		}

		if (!(new File(strCurrentPath).isDirectory())) {
			new File(strCurrentPath).mkdir();
		}
		String[] strFolderList = fn.split("/");

		for (int i = 0; i < strFolderList.length - 1; i++) {
			if (strFolderList[i].equals(""))
				continue;
			strFolderList[i] = strFolderList[i].replaceAll("\\W", "_");
			strCurrentPath = strCurrentPath + File.separatorChar + strFolderList[i];
			if (!(new File(strCurrentPath).isDirectory())) {
				new File(strCurrentPath).mkdir();
			}
		}
		return strCurrentPath;
	}

	public static String createLocalFolder(String strCurrentPath, int TaskID, String strFileName) {
		File f = new File(strCurrentPath);
		if (!f.exists())
			f.mkdirs();

		String[] strFolderList = strFileName.split("/");

		for (int i = 0; i < strFolderList.length; i++) {
			if (strFolderList[i].equals(""))
				continue;
			try {
				strFolderList[i] = new String(strFolderList[i].getBytes("GBK"), "iso-8859-1");
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}

			strFolderList[i] = strFolderList[i].replaceAll("\\W", "_");

			strCurrentPath = strCurrentPath + File.separatorChar + strFolderList[i];
			new File(strCurrentPath).mkdirs();
		}
		return strCurrentPath;
	}

	public static void main(String[] args) {
		// java.util.Date now = new java.util.Date();
		try {
			Date d = Util.getDate1("2010-12-2 1:00:00");
			// String s = ParseFilePath(
			// "/export/local/OMC/DATA/OMP/PMG/asciiPmg/SBS/%%Y%%M%%D/BR90_75.??????????0000+0000.%%Y%%M%%D%%H0000+0000-000.ASCII|3600000",new
			// Timestamp(d.getTime()));
			// String ss =
			// ParseFilePath("/db/GSMV3/PM/100001_MSTAMeasurement_%%d%%EM%%Y_%%NH00-%%d%%EM%%Y_%%H00.csv|-3600000",
			// new Timestamp(d.getTime()));
			// String s = ParseFilePath(
			// "APME/OBSYNT/bsc01/%%Y%%M%%D/R028000(%%H+1).%%DayOfYear",new
			// Timestamp(d.getTime()));
			// String s = ParseFilePath(
			// "/export/local/OMC/DATA/OMP/PMG/asciiPmg/SBS/%%Y%%M%%D/BR90_75.??????????0000+0000.%%Y%%M%%D%%H0000+0000-000.ASCII|3600000%%TA1",new
			// Timestamp(d.getTime()));
			// String s = ParseFilePath(
			// "/export/local/OMC/DATA/OMP/PMG/asciiPmg/SBS/%%Y%%M%%D/BR90_75.??????????0000+0000.%%Y%%M%%D(%%H+1)0000+0000-000.ASCII",new
			// Timestamp(d.getTime()));
			// String s = ParseFilePath(
			// "/export/local/OMC/DATA/OMP/PMG/asciiPmg/SBS/%%Y%%M%%D/BR90_75.??????????0000+0000.%%Y%%M%%D(%%H+1)0000+0000-000.ASCII|3600000%%TA1",new
			// Timestamp(d.getTime())); LSL_2010Dec1_JS_2354_220.bin.zip
			String s = ParseFilePath("/APME/OBSYNT/Zhbsc1/%%Y%%M%%D/R018000(%%H+1).%%DayOfYear", new Timestamp(Util.getDate1("2011-01-01 23:00:00")
					.getTime()));
			System.out.print(s);
			String ss = ParseFilePath("/cdma/lucent/smd_fs/ecp2/%%Y%%M%%D/%%Ny%%NM%%ND%%NH.smdump|0", new Timestamp(Util.getDate1("2015-01-01 00:00:00")
					.getTime()));
			System.out.print(ss);
			;
			
		} catch (ParseException e) {
			e.printStackTrace();
		}
		// //String s = ParseFilePath(
		// "/zcbuff/log/%%DayOfYear_%%EM_mr_detail_%%Y%%M%%d_%%NH0000%%TA1.log.gz|-7200000",new
		// Timestamp(now.getTime()));
		// String s = ParseFilePath(
		// "/export/local/OMC/DATA/OMP/PMG/asciiPmg/SBS/%%Y%%M%%D/BR90_75.??????????0000+0000.%%Y%%M%%D%%H0000+0000-000.ASCII|3600000",new
		// Timestamp(now.getTime()));
		// System.out.print(s);

	}
}
