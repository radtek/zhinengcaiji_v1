package parser.others.gpslog;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.tools.ant.util.DateUtils;

import parser.Parser;
import task.CollectObjInfo;
import task.DevInfo;
import task.RegatherObjInfo;
import util.BalySqlloadThread;
import util.Util;
import collect.FTPConfig;
import framework.SystemConfig;

/**
 * 定位日志分析文件解析
 * 
 * @author liuwx 2010-3-19
 * @version 1.0 1.0.1 liangww 2012-04-27 增加producePath类，并修改parserData函数<br>
 *          1.0.2 liangww 2012-07-27 增加TASK_FILES_RECORDER用于记录处理过的文件
 */
public class CV1Bin extends Parser {

	private static final SystemConfig cfg = SystemConfig.getInstance();

	// 用于记录处理过的文件
	private static final TaskFilesRecorder TASK_FILES_RECORDER = new TaskFilesRecorder(cfg.getCurrentPath(), 1000, 500);;

	String pn2_type = null, match = null, cand = null, p_f = null, lat = null, lon = null, ht = null, hepe = null, prsum = null, pcov = null,
			pphase = null, pn = null, freq = null, band = null, bsid = null, bsidh = null, seqno = null, j36 = null, j36_mktid = null,
			j36_swno = null, j36_bsid = null, j36_bsidh = null, rptr = null, extbsid = null, pn2_sid = null, pn2_nid = null, swno = null, rng = null,
			az = null, mar = null, ctr = null, ori = null, opn = null, pr = null, flc = null, flcu = null;

	public String CLT_GPS_ENSURE_POS_MAIN = "CLT_GPS_ENSURE_POS_MAIN";

	public String CLT_GPS_ENSURE_POS_PN1 = "CLT_GPS_ENSURE_POS_PN1";

	public String CLT_GPS_ENSURE_POS_PN2 = "CLT_GPS_ENSURE_POS_PN2";

	public Map<String, SqlldrParam> paramMap = new HashMap<String, SqlldrParam>();

	public static final int RESULT_OK = 0;

	public static final int RESULT_FAIL = -1;

	// 添加pn详细信息
	private StringBuilder pnLookupPassDetail = new StringBuilder();

	private StringBuilder pnLookupFailDetail = new StringBuilder();

	private String pnSplit = " ";// clt_gps_ensure_pos_main

	// 表中的pn_lookup_pass_detail，
	// pn_lookup_fail_detail字段值用" "分割

	private int mainCount = 0;

	private int pn1Count = 0;

	private int pn2Count = 0;

	private String time = "TIME:(.+)\\s+UNCERTAINTY";

	private String source = "SOURCE:\\s+(\\w+)\\s+";

	private String min = "Primary ID:\\s+(\\d+)";

	private String type = "TYPE:\\s+(.+)\\s+SESSION";

	private String session = "SESSION:\\s(.+)\\s+APPLICATION";

	private String application = "APPLICATION:\\s+(\\d+)";

	private String position_engine = "POSITION ENGINE:\\s+(.+)RESULT";

	private String result = "RESULT:\\s+(.+)\\s+GPS:";

	private String gps = "GPS:\\s+(\\d+)\\s+";

	private String aflt = "AFLT:\\s+(\\d+)\\s+";

	private String eflt = "EFLT:\\s+(\\d+)\\s+";

	private String altitude = "ALTITUDE:\\s+(\\d+)\\s+";

	private String init_latitude = "Init position:\\s+(\\-?[0-9]+\\.[0-9]+)";

	private String init_longitude = "Init position:\\s+\\-?[0-9]+\\.[0-9]+\\s+(\\-?[0-9]+\\.[0-9]+)";// 纬度

	private String init_alt_m = "Init position:\\s+\\-?[0-9]+\\.[0-9]+\\s+\\-?[0-9]+\\.[0-9]+\\s+(\\-?[0-9]+\\.[0-9]+)";// 高度

	private String init_source = "Init position:.+SOURCE:\\s+(.+)\\s+HEPE:";// source

	private String init_hepe_m = "Init position:.+HEPE:(.+)\\s+m";// \\s+

	private String raw_latitude = "Raw position:\\s+(\\-?[0-9]+\\.[0-9]+)";

	private String raw_longitude = "Raw position:\\s+\\-?[0-9]+\\.[0-9]+\\s+(\\-?[0-9]+\\.[0-9]+)";// 纬度

	private String raw_alt_m = "Raw position:\\s+\\-?[0-9]+\\.[0-9]+\\s+\\-?[0-9]+\\.[0-9]+\\s+(\\-?[0-9]+\\.[0-9]+)";// 高度

	private String raw_source = "Raw position:.+SOURCE:\\s+(.+)\\s+HEPE:";// source

	private String raw_hepe_m = "Raw position:.+HEPE:(.+)\\s+m";// \\s+

	private String output_latitude = "Output position:\\s+(\\-?[0-9]+\\.[0-9]+)";

	private String output_longitude = "Output position:\\s+\\-?[0-9]+\\.[0-9]+\\s+(\\-?[0-9]+\\.[0-9]+)";// 纬度

	private String output_alt_m = "Output position:\\s+\\-?[0-9]+\\.[0-9]+\\s+\\-?[0-9]+\\.[0-9]+\\s+(\\-?[0-9]+\\.[0-9]+)";// 高度

	private String output_source = "Output position:.+SOURCE:\\s+(.+)\\s+HEPE:";// source

	private String output_hepe_m = "Output position:.+HEPE:\\s+(.+)\\s+TIME:";

	private String mob_sys_t_offset = "PILOT_PHASE_DATA MOB_SYS_T_OFFSET.{3}:\\s+(\\-?[0-9]+\\.[0-9]+)";//

	private String srv_bs = "SRV_BS:\\s+(.+)NID:";

	private String nid = "NID:\\s+(\\d+)";

	private String sid = "SID:\\s+(\\d+)";

	private String band_freq = "BAND/FREQ:\\s+(.+)REF_PN:";

	private String ref_pn = "REF_PN:\\s(\\d+)";

	private String pwr = "PWR:(.+)$";

	// private static String localTimeRef =
	// "\\*\\*\\*\\s*(.*:\\d+:\\d+\\.\\d+)";
	private int pfpass = 0;

	private int pffail = 0;

	// add
	private Map<String, ArrayList<String>> main = new HashMap<String, ArrayList<String>>();

	private Map<String, ArrayList<String>> pn1 = new HashMap<String, ArrayList<String>>();

	private Map<String, ArrayList<String>> pn2 = new HashMap<String, ArrayList<String>>();

	private ArrayList<String> mainList = new ArrayList<String>();

	private ArrayList<String> pn2List = new ArrayList<String>();

	// private StringBuilder pn1Header = new
	// StringBuilder("LOCAL_TIME,GPS_TIME,GPS_MIN,PN,GPS_TYPE,ELEV,AZIM,RNG_EST,SD_PP,RMSE,RES_PP,RES_TRUTH,PPHASE,EC_I0,EIP,BAND,FREQ,BSID,EXT_ID,P_F");
	private StringBuilder pn1Header = new StringBuilder(
			"LOCAL_TIME,GPS_TIME,GPS_MIN,PN,GPS_TYPE,ELEV,AZIM,RNG_EST,SD_PP,RMSE,RES_PP,RES_TRUTH,PPHASE,EC_I0,EIP,BAND,FREQ,SID,NID,BSID,BSIDH,SEQNO,J36,J36_MKTID,J36_SWNO,J36_BSID,J36_BSIDH,RPTR,P_F");

	static private String pn2Fields = "TYPE,MATCH,CAND,P_F,LAT,LON,HT,HEPE,PRSUM,PCOV,PPHASE,PN,FREQ,BAND,BSID,EXTBSID,SID,NID,SWNO,RNG,AZ,MAR,CTR,ORI,OPN,PR,FLC,FLCU";

	// private String mainHead =
	// "LOCAL_TIME, GPS_TIME, GPS_SOURCE, GPS_MIN, GPS_TYPE, GPS_SESSION, APPLICATION, POSITION_ENGINE, GPS_RESULT, GPS, AFLT, EFLT, ALTITUDE,INIT_LATITUDE,INIT_LONGITUDE,INIT_ALT_M,INIT_SOURCE,INIT_HEPE_M, RAW_LATITUDE, RAW_LONGITUDE,RAW_ALT_M,RAW_SOURCE, RAW_HEPE_M, OUTPUT_LATITUDE, OUTPUT_LONGITUDE,OUTPUT_ALT_M,OUTPUT_SOURCE,OUTPUT_HEPE_M, MOB_SYS_T_OFFSET, SRV_BS, NID, SID, BAND_FREQ, REF_PN, PWR,BS_FOUND_MATCHES,BS_PASS,BS_CELL,BS_SCTR,BS_PN,BS_FREQ,BS_BAND,BS_SID,BS_NID,BS_SWNO,BS_VCNTY,PN_LOOKUP_PASS_NUM,PN_LOOKUP_FAIL_NUM,PN_LOOKUP_PASS_DETAIL,PN_LOOKUP_FAIL_DETAIL";
	private String mainHead = "LOCAL_TIME, GPS_TIME, GPS_SOURCE, GPS_MIN, GPS_TYPE, GPS_SESSION, APPLICATION, POSITION_ENGINE, GPS_RESULT, GPS, AFLT, EFLT, ALTITUDE,INIT_LATITUDE,INIT_LONGITUDE,INIT_ALT_M,INIT_SOURCE,INIT_HEPE_M, RAW_LATITUDE, RAW_LONGITUDE,RAW_ALT_M,RAW_SOURCE, RAW_HEPE_M, OUTPUT_LATITUDE, OUTPUT_LONGITUDE,OUTPUT_ALT_M,OUTPUT_SOURCE,OUTPUT_HEPE_M, MOB_SYS_T_OFFSET, SRV_BS, NID, SID, BAND_FREQ, REF_PN, PWR,"
			+ "BS_FOUND_MATCHES,BS_PASS,BS_SID,BS_NID,BS_BSID,BS_BSIDH,BS_SQNO,BS_J36,BS_J36_MKTID,BS_J36_SWNO,BS_J36_BSID,BS_J36_BSIDH,BS_RPTR,BS_PN,BS_ANTLAT,BS_ANTLON,BS_MAR,BS_SCRNG,BS_ORI,BS_OPN,"
			+ "PN_LOOKUP_PASS_NUM,PN_LOOKUP_FAIL_NUM,PN_LOOKUP_PASS_DETAIL,PN_LOOKUP_FAIL_DETAIL";

	// private String pn2Temp =
	// "GPS_TYPE,MATCH,CAND,P_F,LAT,LON,HT,HEPE,PRSUM,PCOV,PPHASE,PN,FREQ,BAND,BSID,EXTBSID,SID,NID,SWNO,RNG,AZ,MAR,CTR,ORI,OPN,PR,FLC,FLCU";
	private String pn2Temp = "GPS_TYPE,MATCH,CAND,P_F,LAT,LON,HT,HEPE,PRSUM,PCOV,PPHASE,PN,FREQ,BAND,SID,NID,BSID,BSIDH,SEQNO,J36,J36_MKTID,J36_SWNO,J36_BSID,J36_BSIDH,RPTR,RNG,AZ,MAR,CTR,ORI,OPN,FLC,FLCU";

	private CollectObjInfo task;

	private String logKey;

	public CV1Bin() {
		task = super.collectObjInfo;
	}

	public CV1Bin(CollectObjInfo task) {
		this.task = task;
		long id = this.task.getTaskID();
		if (this.task instanceof RegatherObjInfo)
			id = this.task.getKeyID() - 10000000;
		this.logKey = this.task.getTaskID() + "-" + id;
		this.logKey = String.format("[%s][%s]", this.logKey, Util.getDateString(this.task.getLastCollectTime()));

	}

	@Override
	public void setCollectObjInfo(CollectObjInfo collectObjInfo) {
		super.setCollectObjInfo(collectObjInfo);
		this.task = collectObjInfo;
	}

	// 解析定位信息文件
	public void parseData(String filePath) throws Exception {
		/**
		 * 利用bat把bin产生fix,csv文件，其中csv文件中需要解析出localTime, gps_time, min 加它们加到localTimeMap。一边解析fix，一边写sqlldr文件.其中利用拿到gps_time, min反查 localTime,
		 * fix的文件会解析出三个表的数据pn1,pn2,main
		 */
		// 不能解析文件
		if (!canParse(filePath)) {
			return;
		}

		// liangww add 2012-07-27 先通过判断是已经处理过的
		String fileName = new File(filePath).getName();
		TASK_FILES_RECORDER.loadCache(task.getTaskID());
		if (TASK_FILES_RECORDER.containFileName(task.getTaskID(), fileName)) {
			log.info(logKey + "文件：" + filePath + "已经处理过了");
			return;
		}

		//
		Date begin = new Date();
		ProducePath producePath = new ProducePath();
		try {
			// 产生文件
			createFiles(filePath, SystemConfig.getInstance().getTraceFileter2Path(), producePath);
			// 加载csv文件
			// loadCSV(producePath.csvPath);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.info(logKey + "预处理时出现异常", e);
		}

		String lineData = null;
		StringBuilder sb = new StringBuilder();

		String value = null;
		List<String> list = getFieldSign();
		String regex = null;
		boolean mainFlag = false;
		boolean pnFlag1 = false;

		String serv_found = null;
		String serv_match = null;
		String localtime = null;
		String sourcetime = null;
		String minvalue = "0";

		List<Field> fieldList = new ArrayList<Field>();

		String pn2Head = "LOCAL_TIME,GPS_TIME,GPS_MIN," + pn2Temp;// LOCAL_TIME,
		// GPS_TIME,
		// GPS_MIN,
		// pn1,pn2,main表中都有这三个字段
		int pn2Len = pn2Temp.split(",").length;
		ArrayList<String> pn1List = null;

		SqlldrParam param1 = new SqlldrParam();
		param1.tbName = CLT_GPS_ENSURE_POS_MAIN;
		commonSqlldr(param1, CLT_GPS_ENSURE_POS_MAIN, fieldList, mainHead);
		SqlldrLog sqlldrMain = new SqlldrLog(param1);
		sqlldrMain.execute();
		param1.records = main;// pn1;

		// runSqlldr(param1, CLT_GPS_ENSURE_POS_MAIN, fieldList, mainHead);

		SqlldrParam parPn1 = new SqlldrParam();
		parPn1.tbName = CLT_GPS_ENSURE_POS_PN1;
		commonSqlldr(parPn1, CLT_GPS_ENSURE_POS_PN1, fieldList, pn1Header.toString());
		parPn1.records = pn1;// pn1;
		SqlldrLog sqlldrPn1 = new SqlldrLog(parPn1);
		sqlldrPn1.execute();
		// runSqlldr(param2, CLT_GPS_ENSURE_POS_PN1, fieldList,
		// pn1Header.toString());

		SqlldrParam paraPn2 = new SqlldrParam();
		paraPn2.tbName = CLT_GPS_ENSURE_POS_PN2;
		commonSqlldr(paraPn2, CLT_GPS_ENSURE_POS_PN2, fieldList, pn2Head);
		paraPn2.records = pn2;// pn1;
		SqlldrLog sqlldrPn2 = new SqlldrLog(paraPn2);
		sqlldrPn2.execute();

		// runSqlldr(param3, CLT_GPS_ENSURE_POS_PN2, fieldList, pn2Head);

		BufferedReader br = null;
		String tempLineData = null;
		// 一行一行读取
		try {
			br = new BufferedReader(new InputStreamReader(new FileInputStream(producePath.fixPath)));
			while (tempLineData == null ? ((lineData = br.readLine()) != null) : tempLineData != null) {
				try {
					if (tempLineData != null) {
						lineData = tempLineData;
						tempLineData = null;
					}
					sb.append(lineData + "\r\n");

					// 主表信息处理
					if (lineData.contains("PWR:")) {
						boolean bError = true;
						mainList = new ArrayList<String>();
						localtime = null;
						for (int i = 0; i < list.size(); i++) {
							regex = list.get(i);
							value = null;
							value = findByRegex(sb.toString(), regex, 1);
							if (regex.contains(time)) {
								// GPSTime转换成北京时间 modified by yuy,2013-07-02
								localtime = GPSTimeSwitch.convertGPSTimeToLocalTime(value);
								if (localtime != null)
									bError = false;
								else {
									log.warn("content = '" + sb.toString() + "',此条数据被丢弃");
									break;
								}

								sourcetime = value.replace(",", "").trim();

								mainList.add(localtime);
								mainList.add(sourcetime);
							} else if (regex.contains(min)) {
								minvalue = value;
								mainList.add(value);
							} else {
								mainList.add(value);
							}
						}

						// 如果出异常清空
						if (bError)
							mainList.clear();

						mainFlag = !bError;	// 与bError相反
						sb.delete(0, sb.length());
						continue;
					}
					// PN1表处理
					if (mainFlag) {
						// String regEx = "Serving BS.+\r\nServing BS.+\r\n";
						String regEx2 = "Serv. - Found.+\r\n(Serv.\r\n)?";
						String regEx3 = "Serv. - Match.+";
						String regEx4 = "Serv. - BS.+";
						if ((lineData.contains("Serv. - Match") || lineData.contains("Serv. - BS"))
								&& (serv_found = findByRegex(sb.toString(), regEx2, 0)) != null) {
							String[] bs = serv_found.split("\r\n");
							String foundline = bs[0];

							serv_match = findByRegex(sb.toString(), regEx3, 0);
							if (serv_match == null) {
								serv_match = findByRegex(sb.toString(), regEx4, 0);
							}
							String[] bs0 = serv_match.split("\r\n");
							String matchline = bs0[0];

							String BS_FOUND_MATCHES = foundline.substring(foundline.indexOf("Found") + 5, foundline.indexOf("CLM")).trim();

							// 填写Found 1中的1（可能是0到N，目前我们取0或者1，其他N都当1）
							if (!"0".equals(BS_FOUND_MATCHES)) {
								BS_FOUND_MATCHES = "1";
							}

							// 只要Found N中，N非0 即认为是pass，填0，但如果Found N后跟了fail，则认为fail，填1。当为Found 0则认为是fail。直接填1
							String BS_PASS = "0".equals(BS_FOUND_MATCHES) ? "1" : "0";
							if ("fail".equals(matchline.substring(foundline.indexOf("CLM") - 1, foundline.indexOf("CLM") + 3).trim())) {
								BS_PASS = "1";
							}
							String BS_SID = matchline.substring(foundline.indexOf("IS801") + 5, foundline.indexOf("SID") + 3).trim();
							String BS_NID = matchline.substring(foundline.indexOf("SID") + 3, foundline.indexOf("NID") + 3).trim();
							String BS_BSID = matchline.substring(foundline.indexOf("NID") + 3, foundline.indexOf("BSIDd") + 5).trim();
							String BS_BSIDH = matchline.substring(foundline.indexOf("BSIDd") + 5, foundline.indexOf("BSIDh") + 5).trim();
							String BS_SQNO = matchline.substring(foundline.indexOf("BSIDh") + 5, foundline.indexOf("SqNo") + 4).trim();
							String BS_J36 = matchline.substring(foundline.indexOf("SqNo") + 4, foundline.indexOf("J36") + 3).trim();
							String BS_J36_MKTID = matchline.substring(foundline.indexOf("J36") + 3, foundline.indexOf("MktID") + 5).trim();
							String BS_J36_SWNO = matchline.substring(foundline.indexOf("MktID") + 5, foundline.indexOf("SwNo") + 4).trim();
							String BS_J36_BSID = matchline.substring(foundline.indexOf("SwNo") + 4, foundline.lastIndexOf("BSIDd") + 5).trim();
							String BS_J36_BSIDH = matchline.substring(foundline.lastIndexOf("BSIDd") + 5, foundline.lastIndexOf("BSIDh") + 5).trim();
							String BS_RPTR = matchline.substring(foundline.lastIndexOf("BSIDh") + 5, foundline.indexOf("Rptr") + 4).trim();
							String BS_PN = matchline.substring(foundline.indexOf("Rptr") + 4, foundline.indexOf("PN") + 2).trim();
							String BS_ANTLAT = matchline.substring(foundline.indexOf("PN") + 2, foundline.indexOf("AntLat") + 6).trim();
							String BS_ANTLON = matchline.substring(foundline.indexOf("AntLat") + 6, foundline.indexOf("AntLon") + 6).trim();
							String BS_MAR = matchline.substring(foundline.indexOf("AntLon") + 6, foundline.indexOf("Mar") + 3).trim();
							String BS_SCRNG = matchline.substring(foundline.indexOf("Mar") + 3, foundline.indexOf("SCRng") + 5).trim();
							String BS_ORI = matchline.substring(foundline.indexOf("SCRng") + 5, foundline.indexOf("Ori") + 3).trim();
							String BS_OPN = matchline.substring(foundline.indexOf("Ori") + 3, foundline.indexOf("Opn") + 3).trim();

							mainList.add(BS_FOUND_MATCHES);
							mainList.add(BS_PASS);
							mainList.add(BS_SID);
							mainList.add(BS_NID);
							mainList.add(lineHandler(BS_BSID));
							mainList.add(lineHandler(BS_BSIDH));
							mainList.add(lineHandler(BS_SQNO));
							mainList.add(BS_J36);
							mainList.add(lineHandler(BS_J36_MKTID));
							mainList.add(lineHandler(BS_J36_SWNO));
							mainList.add(lineHandler(BS_J36_BSID));
							mainList.add(lineHandler(BS_J36_BSIDH));
							mainList.add(lineHandler(BS_RPTR));
							mainList.add(lineHandler(BS_PN));
							mainList.add(BS_ANTLAT);
							mainList.add(BS_ANTLON);
							mainList.add(BS_MAR);
							mainList.add(lineHandler(BS_SCRNG));
							mainList.add(lineHandler(BS_ORI));
							mainList.add(lineHandler(BS_OPN));

							String[] matchResult = sb.toString().trim().split("\r\n\r\n");
							String pn1Data = null;
							if (matchResult.length > 0) {
								pn1Data = matchResult[0];
							}
							if (pn1Data != null) {
								String[] pn1Datas = pn1Data.split("\r\n");
								for (int i = 1; i < pn1Datas.length; i++) {
									pn1List = new ArrayList<String>();
									String line = pn1Datas[i].trim();
									if (line == null || "".equals(line.trim()))
										continue;

									pn1List.add(localtime);
									pn1List.add(sourcetime);
									pn1List.add(minvalue);

									String[] data = line.split("\\s+");
									String pn = null;
									boolean pfFlag = false;
									for (int j = 0; j < data.length; j++) {
										pn = data[j];
										if ("IS801".equals(pn))
											continue;
										if (pn.contains("--") || "-".equals(pn))
											pn = "";
										if (pn.indexOf("*") != -1 && pn.charAt(0) != '*') {
											pn = pn.substring(0, pn.indexOf("*"));
											pfFlag = true;
										}
										int star = 0;
										if (!"".equals(pn) && pn.charAt(0) == '*') {
											star++;
											for (int k = 1; k < pn.length(); k++) {
												char c = pn.charAt(k);
												if (c == '*')
													star++;

											}
											if (star == pn.length()) {
												pn = null;
											}
										}
										pn1List.add(Util.isNull(pn) ? "" : pn.trim());
									}
									if (pfFlag) {
										pn1List.add("PASS");
									} else {
										pn1List.add("FAIL");
									}

									pn1.put("pn1" + (pn1Count++), pn1List);
									if (pn1.size() > SqlldrLog.bufferSize) {
										sqlldrPn1.isOut(parPn1);
										pn1.clear();
									}

								}
								pnFlag1 = true;
								sb.delete(0, sb.length());
								// 拼接main主表sql ，当pn2没有数据时，对main的处理方式。这里读两行
								String tempLineData0 = br.readLine().trim();
								tempLineData0 = br.readLine().trim();

								/** 会出现多个“Serv. -”开头的或者空行，忽略掉。--20141218 */
								while (tempLineData0.startsWith("Serv. -") || tempLineData0.length() == 0) {
									tempLineData0 = br.readLine().trim();
								}

								if (tempLineData0.startsWith("TIME:"))// 新的一条记录，pn2没有数据
								{
									tempLineData = tempLineData0;
									mainFlag = false;
									pnFlag1 = false;
									pfpass = 0;
									pffail = 0;

									mainList.add(String.valueOf(pfpass));
									mainList.add(String.valueOf(pffail));

									// pnLookupPassDetail
									mainList.add(String.valueOf(0));
									// pnLookupFailDetail
									mainList.add(String.valueOf(0));

									main.put(String.valueOf(mainCount++), new ArrayList<String>(mainList));
									// param1.records.put(String.valueOf(mainCount++),
									// mainList);
									if (main.size() > SqlldrLog.bufferSize) {
										sqlldrMain.isOut(param1);
										main.clear();
									}
									mainList.clear();
									continue;
								}
								continue;
							}
						}
						// PN信息表2处理
						if (pnFlag1 && sb.toString().trim().length() > 0 && sb.toString().contains("\r\n\r\n")) {
							String[] pn1temp = sb.toString().split("\r\n");

							List<String> fList = Arrays.asList(pn2Fields.split(","));
							int bsidIndex = 0;
							if (fList.contains("BSID"))
								bsidIndex = fList.indexOf("BSID");

							processPn2(pn1temp, pn2Fields, localtime, sourcetime, minvalue, pn2Len, bsidIndex, paraPn2, sqlldrPn2);

							sb.delete(0, sb.length());
							mainFlag = false;
							pnFlag1 = false;

							// 拼接main主表sql
							mainList.add(String.valueOf(pfpass));
							mainList.add(String.valueOf(pffail));

							if (pnLookupPassDetail.length() > 0) {
								pnLookupPassDetail.deleteCharAt(pnLookupPassDetail.length() - 1);

								mainList.add(pnLookupPassDetail.toString());
							} else {
								mainList.add("");
							}
							if (pnLookupFailDetail.length() > 0) {
								pnLookupFailDetail.deleteCharAt(pnLookupFailDetail.length() - 1);
								mainList.add(pnLookupFailDetail.toString());
							} else {
								mainList.add("");
							}

							main.put(String.valueOf(mainCount++), mainList);

							if (main.size() > SqlldrLog.bufferSize) {
								sqlldrMain.isOut(param1);
								main.clear();
							}

							pnLookupPassDetail.delete(0, pnLookupPassDetail.length());
							pnLookupFailDetail.delete(0, pnLookupFailDetail.length());

							pfpass = 0;
							pffail = 0;
							continue;
						}
					}
				} catch (IOException e) {
					log.info(logKey + "解析过程中出异常", e);
				}
			}
			log.info(task.getTaskID() + ":" + filePath + " 文件解析成功，解析耗时：" + (int) (new Date().getTime() - begin.getTime()) / 1000 + "秒");

			if (mainCount == 0 && pn1Count == 0 && pn2Count == 0)
				return;

			if (param1.records.size() > 0) {
				sqlldrMain.out(param1);

			}
			param1.clear();
			if (parPn1.records.size() > 0) {
				sqlldrPn1.out(parPn1);
			}
			parPn1.clear();
			if (paraPn2.records.size() > 0) {
				sqlldrPn2.out(paraPn2);
			}
			paraPn2.clear();

			begin = new Date();

			sqlldrMain.run();
			sqlldrPn1.run();
			sqlldrPn2.run();

			log.info(task.getTaskID() + ":" + filePath + " 文件入库成功，入库耗时：" + (int) (new Date().getTime() - begin.getTime()) / 1000 + "秒");

			// liangww add 2012-07-27 记录这个文件已经处理过
			TASK_FILES_RECORDER.addFileName(task.getTaskID(), fileName);
		} catch (IOException e) {
			log.info(logKey + "sqlldr run", e);
		} finally {
			if (br != null)
				try {
					br.close();
				} catch (IOException e) {
				}

			// 删除.txt ,.csv ,.mlb ,.bat,.fix,bin文件
			FTPConfig fconfig = FTPConfig.getFTPConfig(task.getTaskID());
			boolean bb = false;
			if (fconfig != null)
				bb = fconfig.isDelete();
			if (SystemConfig.getInstance().isDeleteLog() || bb) {
				try {
					String msg = producePath.deleteAllFile();
					log.debug(task + msg);
				} catch (Exception e) {
					log.error(task.getTaskID() + ": Delete GPS Log 文件出现异常 ");
				}
			}
		}

	}

	private String lineHandler(String str) {
		if (str == null)
			return "";
		if ("-".equals(str))
			return "";
		return str.contains("--") ? "" : str;
	}

	public void commonSqlldr(SqlldrParam param, String tableName, List<Field> fieldList, String headString) {
		param.taskID = task.getTaskID();
		// param.tbName = tableName;
		param.omcID = task.getDevInfo().getOmcID();
		// param.records = pn2;// pn1;
		param.commonFields = fieldList;
		param.dataTime = task.getLastCollectTime();// pn1Header
		param.head = headString.toString().replace("LOCAL_TIME", "LOCAL_TIME Date 'YYYY-MM-DD HH24:MI:SS'");
		param.headTxt = headString.toString().replace(",", ";");
		param.beginTime = DateUtils.format(new Date(), "yyyy-mm-dd hh:mm:ss");
		param.tableIndex = 0;
	}

	private int getIndex(String pnFields, String foundField) {
		List<String> fList = Arrays.asList(pnFields.split(","));
		int bsidIndex = 0;
		for (int ii = 0; ii < fList.size(); ii++) {
			if (foundField.equals(fList.get(ii))) {
				bsidIndex = ii;
				break;
			}
		}
		return bsidIndex;
	}

	/**
	 * 获得pn2 sql语句
	 * 
	 * @param pn1Datas
	 * @param sql
	 * @param fields
	 * @param localtime
	 * @param sourcetime
	 * @param min
	 * @return
	 */
	public void processPn2(String[] pn1Datas, String fields, String localtime, String sourcetime, String min, int pn2Len, int bsidIndex,
			SqlldrParam paraPn2, SqlldrLog sqlldrPn2) {
		// "2010-03-21 08:08:00";
		int pnFlag = -1;// -1:null,0:fail,1:pass

		for (int i = 0; i < pn1Datas.length; i++) {
			pn2List = new ArrayList<String>();
			String line = pn1Datas[i];
			if (line == null || line.equals(""))
				continue;
			try {
				pn2List.add(localtime);
				pn2List.add(Util.isNull(sourcetime) ? "" : sourcetime);//
				pn2List.add(Util.isNull(min) ? "" : min);//

				pn2_type = line.substring(3, 8);
				if (pn2_type.contains("Unkno"))// Unknown
				{
					pn2_type = line.substring(3, 10);
					match = line.substring(10, 15);
					cand = line.substring(15, 19);
					p_f = line.substring(20, 24);
					lat = line.substring(24, 34);
					lon = line.substring(34, 45);
					ht = line.substring(45, 50);
					hepe = line.substring(50, 56);
					prsum = line.substring(56, 62);
					pcov = line.substring(62, 68);
					pphase = line.substring(68, 75);
					pn = line.substring(75, 80);
					freq = line.substring(80, 85);
					band = line.substring(85, 89);
				} else {
					match = line.substring(9, 13);
					cand = line.substring(14, 17);
					p_f = line.substring(17, 22);
					lat = line.substring(23, 32);
					lon = line.substring(33, 43);
					ht = line.substring(44, 48);
					hepe = line.substring(49, 55);
					prsum = line.substring(55, 61);
					pcov = line.substring(61, 67);
					pphase = line.substring(67, 74);
					pn = line.substring(74, 79);
					freq = line.substring(79, 84);

					band = line.substring(83, 87);
				}

				// if(cand != null && cand.trim().length()>2)
				// System.out.println(cand);

				pn2List.add(Util.isNull(pn2_type) ? "" : pn2_type);//
				pn2List.add(Util.isNull(match) ? "" : match);//
				pn2List.add(Util.isNull(cand) ? "" : cand);//
				pn2List.add(Util.isNull(p_f) ? "" : p_f);//
				pn2List.add(Util.isNull(lat) ? "" : lat);//
				pn2List.add(Util.isNull(lon) ? "" : lon);//
				pn2List.add(Util.isNull(ht) ? "" : ht);//
				pn2List.add(Util.isNull(hepe) ? "" : hepe);//
				pn2List.add(Util.isNull(prsum) ? "" : prsum);//
				pn2List.add(Util.isNull(pcov) ? "" : pcov);//
				pn2List.add(Util.isNull(pphase) ? "" : pphase);//
				pn2List.add(Util.isNull(pn) ? "" : pn);//
				pn2List.add(Util.isNull(freq) ? "" : freq);//
				pn2List.add(Util.isNull(band) ? "" : band);//

				// 保存pass or fail 失败信息
				if (p_f != null) {
					if (p_f.trim().equalsIgnoreCase("pass")) {
						pnFlag = 1;
						pfpass++;
					} else if (p_f.trim().equalsIgnoreCase("FAIL")) {
						pnFlag = 0;
						pffail++;
					}
				}
				// 保存pn信息
				pn = (pn == null) ? null : pn.trim();
				if (pnFlag == 1)
					pnLookupPassDetail.append(pn).append(pnSplit);
				else if (pnFlag == 0)
					pnLookupFailDetail.append(pn).append(pnSplit);
				pnFlag = -1;

				if (line.length() < 90) {
					int appendNullNum = pn2Len - getIndex(fields, "BSID");
					for (int m = 0; m < appendNullNum; m++) {
						pn2List.add("");
					}
					pn2.put("pn2 " + pn2Count++, pn2List);
					clear();
					continue;
				}

				// String IS801 = line.substring(87, 92);

				pn2_sid = line.substring(94, 100);
				pn2List.add(Util.isNull(pn2_sid) ? "" : pn2_sid);//

				pn2_nid = line.substring(100, 106);
				pn2List.add(Util.isNull(pn2_nid) ? "" : pn2_nid);//

				bsid = line.substring(106, 112);
				pn2List.add(Util.isNull(bsid) ? "" : bsid);//

				bsidh = line.substring(112, 119);
				pn2List.add(Util.isNull(bsidh) ? "" : bsidh);//

				seqno = line.substring(119, 125);
				pn2List.add(Util.isNull(seqno) ? "" : seqno);//

				j36 = line.substring(125, 129);
				pn2List.add(Util.isNull(j36) ? "" : j36);//

				j36_mktid = line.substring(129, 135);
				pn2List.add(lineHandler(j36_mktid));//

				j36_swno = line.substring(135, 141);
				pn2List.add(lineHandler(j36_swno));//

				j36_bsid = line.substring(141, 147);
				pn2List.add(lineHandler(j36_bsid));//

				j36_bsidh = line.substring(147, 154);
				pn2List.add(lineHandler(j36_bsidh));//

				rptr = line.substring(154, 160);
				pn2List.add(Util.isNull(rptr) ? "" : rptr);//

				rng = line.substring(160, 166);
				pn2List.add(Util.isNull(rng) ? "" : rng);//

				if (line.length() < 176)//
				{
					az = line.substring(166, 171);
					pn2List.add(Util.isNull(az) ? "" : az);//

					int appendNullNum = pn2Len - getIndex(fields, "MAR");;
					for (int m = 0; m < appendNullNum; m++) {
						pn2List.add("");
					}
					pn2.put("pn2 " + pn2Count++, pn2List);
					clear();
					continue;
				}
				az = line.substring(166, 171);
				pn2List.add(Util.isNull(az) ? "" : az);//

				mar = line.substring(171, 177);
				pn2List.add(Util.isNull(mar) ? "" : mar);//

				ctr = line.substring(177, 183);
				pn2List.add(Util.isNull(ctr) ? "" : ctr);//

				ori = line.substring(183, 188);
				pn2List.add(Util.isNull(ori) ? "" : ori);//

				opn = line.substring(188, 192);
				pn2List.add(Util.isNull(opn) ? "" : opn);//

				flc = line.substring(192, 197).replace("-", "");
				pn2List.add(Util.isNull(flc) ? "" : flc);//

				flcu = line.substring(197, line.length());
				pn2List.add(Util.isNull(flcu) ? "" : flcu);//

				// if(!flcu.trim().matches("^[-+]?(([0-9]+)([.]([0-9]+))?|([.]([0-9]+))?)$")){
				// System.out.println(pn+" "+flcu);
				// }

				pn2.put("pn2 " + pn2Count++, pn2List);

				if (pn2.size() > SqlldrLog.bufferSize) {
					sqlldrPn2.isOut(paraPn2);
					pn2.clear();
				}
				clear();
			} catch (Exception e) {
				log.error("解析入表CLT_GPS_ENSURE_POS_PN2数据时异常，line=" + line, e);
				continue;
			}
		}
	}

	private void clear() {
		p_f = null;
		lat = null;
		lon = null;
		ht = null;
		hepe = null;
		prsum = null;
		pcov = null;
		pphase = null;
		pn = null;
		freq = null;
		band = null;
		bsid = null;
		extbsid = null;
		pn2_sid = null;
		pn2_nid = null;
		swno = null;
		rng = null;
		az = null;
		mar = null;
		ctr = null;
		ori = null;
		opn = null;
		pr = null;
		flc = null;
		flcu = null;
	}

	// 取得表字段标记
	private List<String> getFieldSign() {
		List<String> list = new ArrayList<String>();
		list.add(time);
		list.add(source);
		list.add(min);
		list.add(type);
		list.add(session);
		list.add(application);
		list.add(position_engine);
		list.add(result);
		list.add(gps);
		list.add(aflt);
		list.add(eflt);
		list.add(altitude);
		list.add(init_latitude);
		list.add(init_longitude);// 纬度
		list.add(init_alt_m);//

		list.add(init_source);// source
		list.add(init_hepe_m);// \\s+
		list.add(raw_latitude);
		list.add(raw_longitude);// 纬度
		list.add(raw_alt_m);//

		list.add(raw_source);// source
		list.add(raw_hepe_m);
		list.add(output_latitude);
		list.add(output_longitude);// 纬度
		list.add(output_alt_m);//
		list.add(output_source);// source

		list.add(output_hepe_m);
		list.add(mob_sys_t_offset);//
		list.add(srv_bs);
		list.add(nid);
		list.add(sid);
		list.add(band_freq);
		list.add(ref_pn);
		list.add(pwr);
		return list;
	}

	// 通过正则表达式查找
	private String findByRegex(String str, String regEx, int group) {
		String resultValue = null;
		if (regEx == null || (regEx != null && "".equals(regEx.trim()))) {
			return resultValue;
		}
		Pattern p = Pattern.compile(regEx);
		Matcher m = p.matcher(str);

		boolean result = m.find();// 查找是否有匹配的结果
		if (result) {
			resultValue = m.group(group);// 找出匹配的结果
		}
		return resultValue;
	}

	/**
	 * 加载load csv，主要是以gpsTime min作为key, LOCAL_TIME作为value
	 * 
	 * @param csvPathtrim
	 * @throws IOException
	 */
	public void loadCSV(String csvPath) throws IOException {
		// String mlbLine = null;
		// BufferedReader mlbReader = null;
		//
		// mlbReader = new BufferedReader(new InputStreamReader(new FileInputStream(csvPath)));
		// //错过第一行
		// mlbReader.readLine();
		// while ((mlbLine = mlbReader.readLine()) != null)
		// {
		// String[] values = mlbLine.split(",");
		// if(values.length < 3)
		// {
		// continue;
		// }
		//
		// String gpsTime = values[0];
		// String localTime = values[1];
		// String min = values[2];
		//
		// String key = getKey(gpsTime, min);
		// try
		// {
		// String value = formatDateStr(localTime);
		// this.localTimeMap.put(key, value);
		// }
		// catch (ParseException e)
		// {
		// // TODO Auto-generated catch block
		// log.warn("localTime:"+localTime+" paser error" ,e);
		// }
		// }

	}

	/**
	 * 获取key
	 * 
	 * @param gpsTime
	 * @param min
	 * @return
	 */
	static String getKey(String gpsTime, String min) {
		return gpsTime + '_' + min;
	}

	/**
	 * 格式化日期字符串
	 * 
	 * @param localTime
	 * @return
	 * @throws ParseException
	 */
	static String formatDateStr(String localTime) throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat("EEE MMM dd HH:mm:ss yyyy", Locale.US);
		SimpleDateFormat formatSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return formatSdf.format(sdf.parse(localTime));
	}

	/**
	 * 能否解析文件
	 * 
	 * @param fileName
	 * @return
	 */
	boolean canParse(String fileName) {
		log.debug(task + ":  开始Gps定位日志文件解析，文件：" + fileName);
		File file = new File(fileName);
		if (!file.exists()) {
			log.error(task + ":  开始Gps定位日志文件解析，文件未找到：" + fileName);
			return false;
		}
		if (fileName.endsWith(".fix")) {
			log.debug("the file is fix file,it could be bin file  ,so abort parse it-------------------");
			return false;
		}

		return true;
	}

	/**
	 * 生成文件
	 * 
	 * @param fileName
	 * @param traceFilter2Path
	 * @param producePath
	 * @throws Exception
	 */
	void createFiles(String fileName, String traceFilter2Path, ProducePath producePath) throws Exception {
		// ${traceFilter2Path} /f ${binPath} /ab /h /y /fix /nr all /nm /nt
		// ${ParseFix} -r {$fixPath}
		String contentFormat = "%s /f %s /ab  /h  /y  /fix  /nr all  /nm /nt";
		String content = null;
		File f = new File(fileName);
		String tname = f.getAbsolutePath();
		String fileNameTem = tname.substring(0, tname.lastIndexOf("."));

		producePath.fixPath = fileNameTem + ".fix";
		producePath.mlbPath = fileNameTem + ".mlb";
		producePath.mlbPath = fileNameTem + ".csv";
		// 获取文件名，不包含后缀,生成bat内容
		producePath.batPath = fileNameTem + ".bat";

		content = String.format(contentFormat, traceFilter2Path, f.getAbsolutePath());
		log.debug(task + ":  shellPreare：" + content);

		// File txttempfile = new File(producePath.batPath);
		// //如果之前存在，删除
		// if ( txttempfile.exists() )
		// {
		// txttempfile.delete();
		// }
		// txttempfile.createNewFile();
		// //生成bat
		// BufferedWriter bw;
		// bw = new BufferedWriter(new FileWriter(producePath.batPath, false));
		// try{
		// bw.write(content);
		// }
		// finally{
		// try{bw.close();}catch (Exception e) {}
		// }

		// 运行bat
		BalySqlloadThread thread = new BalySqlloadThread();
		// thread.runcmd(producePath.batPath);
		thread.runcmd(content);
	}

	/**
	 * 
	 * ProducePath
	 * 
	 * @author liangww 2012-4-27
	 */
	public static class ProducePath {

		String batPath = null;			// bat文件路径

		String oldCsvPath = null;		// 旧的csv路径

		String binPath = null;			// bin文件路径

		String csvPath = null;			// csv文件路径

		String fixPath = null;			// fix文件路径

		String mlbPath = null;			// mlb文件路径

		/**
		 * 删除所有文件
		 * 
		 * @param log
		 * @throws Exception
		 */
		public String deleteAllFile() throws Exception {
			StringBuilder buf = new StringBuilder(": 删除GPS文件成功");
			java.lang.reflect.Field[] fields = this.getClass().getDeclaredFields();

			for (int i = 0; i < fields.length; i++) {
				fields[i].setAccessible(true);
				Object value = fields[i].get(this);
				if (value == null)
					continue;

				File csv = new File(value.toString());
				if (csv.exists()) {
					buf.append("  " + fields[i].getName()).append(":" + csv.getAbsolutePath());
					csv.delete();
				}
			}// for (int i = 0; i < fields.length; i++)

			return buf.toString();
		}
	}

	public static void main(String[] args) throws Exception {
		Date date = new Date();

		String fileName = "F:\\datacollector_path\\222223\\test\\yuyi\\ceshi\\lsl_20130707_3.bin";
		CollectObjInfo obj = new CollectObjInfo(755123);
		obj.setLastCollectTime(new Timestamp(new Date().getTime()));
		DevInfo dev = new DevInfo();
		dev.setOmcID(100);
		obj.setDevInfo(dev);
		CV1Bin pe = new CV1Bin(obj);
		pe.task = obj;
		pe.parseData(fileName);
		System.out.println("解析入库花费了：" + (System.currentTimeMillis() - date.getTime()) / 1000 + "秒");
		System.exit(0);
		// String str = null;
		// String regEx2 = "Serv. - Found.+\r\n(Serv.\r\n)?";
		// String regEx3 = "Serv. - Match.+";
		// String lineData = "ID: 54 (AfltPilot)   Stage: SA1    SMO  fault   :    -22.200 m   action: SMO isolated"+
		// "Serv. - Looking for...         IS801 13828     3  6641 0x19f1 ----- J36 ----- ----- ----- ------ ----- --- ---------- ---------- ------ ----- --- ---"+
		// "Serv. - Found  1           CLM IS801   SID   NID BSIDd  BSIDh  SqNo J36 MktID  SwNo BSIDd  BSIDh  Rptr  PN     AntLat     AntLon    Mar SCRng Ori Opn\r\n"+
		// "Serv. - Match  1 of  1         IS801 13828     3  6641 0x19f1     0 J36 ----- ----- ----- ------     0 274   23.04264  113.32521   2708   542 215 130\r\n"+"1111";
		// if ( lineData.contains("Serv. - Match") && (str = new CV1Bin().findByRegex(lineData, regEx2, 0)) != null)
		// {
		// System.out.println(str);
		// System.out.println(new CV1Bin().findByRegex(lineData, regEx3, 0));
		// }
		// System.out.println("2222222222222222222");
		// String[] pn1temp = {"13 Meas.   0    9 FAIL  33.61113  119.04905    0   443  0.00               417  283   0",
		// " 3 Meas.   1    3 pass  33.53919  119.13061    0  1176  1.00               183  201   0",
		// "24 Unknown   0    0 FAIL  33.47545  119.83039    0   849  0.00               489  242   0",
		// " 7 Unknown   0    0 FAIL  33.54092  119.13304    0  1145  0.00               183  201   0"};
		// List<String> fList = Arrays.asList(pn2Fields.split(","));
		// int bsidIndex = 0;
		// if(fList.contains("BSID"))
		// bsidIndex = fList.indexOf("BSID");
		// new CV1Bin().processPn2(pn1temp, pn2Fields, null, null, null, pn2Fields.length(), bsidIndex, null, null);
	}

	@Override
	public boolean parseData() throws Exception {
		parseData(fileName);
		return true;
	}
}
