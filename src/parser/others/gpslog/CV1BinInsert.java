package parser.others.gpslog;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import collect.FTPConfig;

import parser.Parser;
import task.CollectObjInfo;
import task.DevInfo;
import util.BalySqlloadThread;
import util.DBLogger;
import util.DbPool;
import util.LogMgr;
import util.Util;
import framework.SystemConfig;

/**
 * 定位日志分析文件解析
 * 
 * @author liuwx 2010-3-19
 */
public class CV1BinInsert extends Parser {

	private final int batchSize = 2000;

	String pn2_type = null, match = null, cand = null, p_f = null, lat = null, lon = null, ht = null, hepe = null, prsum = null, pcov = null,
			pphase = null, pn = null, freq = null, band = null, bsid = null, extbsid = null, pn2_sid = null, pn2_nid = null, swno = null, rng = null,
			az = null, mar = null, ctr = null, ori = null, opn = null, pr = null, flc = null, flcu = null;

	public String CLT_GPS_ENSURE_POS_MAIN = "CLT_GPS_ENSURE_POS_MAIN";

	public String CLT_GPS_ENSURE_POS_PN1 = "CLT_GPS_ENSURE_POS_PN1";

	public String CLT_GPS_ENSURE_POS_PN2 = "CLT_GPS_ENSURE_POS_PN2";

	public List<String> mainSqlList = null;

	public List<String> pn1SqlList = null;

	public List<String> pn2SqlList = null;

	// 添加pn详细信息
	private StringBuilder pnLookupPassDetail = new StringBuilder();

	private StringBuilder pnLookupFailDetail = new StringBuilder();

	private String pnSplit = ";";

	//

	private int mainCount = 0;

	private int pn1Count = 0;

	private int pn2Count = 0;

	private static DBLogger dbLogger = LogMgr.getInstance().getDBLogger();

	private String time = "TIME:(.+)\\s+UNCERTAINTY";

	private String source = "SOURCE:\\s+(\\w+)\\s+";

	private String min = "MIN:\\s+(\\d+)";

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

	private String output_hepe_m = "Output position:.+HEPE:(.+)";

	private String mob_sys_t_offset = "PILOT_PHASE_DATA MOB_SYS_T_OFFSET.{3}:\\s+(\\-?[0-9]+\\.[0-9]+)";//

	private String srv_bs = "SRV_BS:\\s+(.+)NID:";

	private String nid = "NID:\\s+(\\d+)";

	private String sid = "SID:\\s+(\\d+)";

	private String band_freq = "BAND/FREQ:\\s+(.+)REF_PN:";

	private String ref_pn = "REF_PN:\\s(\\d+)";

	private String pwr = "PWR:(.+)$";

	// private String baseOnTime = "1960-04-25 0:00:00";
	private String baseOnTime = "1960-06-12 23:33:59";

	// private static String localTimeRef =
	// "\\*\\*\\*\\s*(.*:\\d+:\\d+\\.\\d+)";
	Map<String, String> localTimeMap = new HashMap<String, String>();

	private int pfpass = 0;

	private int pffail = 0;

	public CV1BinInsert() {
	}

	public CV1BinInsert(CollectObjInfo collectObjInfo) {
		super(collectObjInfo);

	}

	// 解析定位信息文件
	public void parse(String fileName) throws BatchUpdateException {
		File file = new File(fileName);
		if (!file.exists()) {
			log.error(collectObjInfo + ":  开始Gps定位日志文件解析，文件未找到：" + fileName);
			return;
		}
		String traceFilter2Path = SystemConfig.getInstance().getTraceFileter2Path();

		String batDir = fileName.substring(0, fileName.lastIndexOf("\\"));
		String fileNameTem = fileName.substring(fileName.lastIndexOf("\\") + 1, fileName.lastIndexOf(".bin"));
		String batFile = batDir + File.separator + fileNameTem + ".bat";

		String shellPreare = "/f " + batDir + File.separator + fileNameTem + ".bin /fix";// taskInfo.get_ShellCmdPrepare();
		String bat = traceFilter2Path + " " + shellPreare;

		String fixPath = batDir + File.separator + fileNameTem + ".fix";
		String mlbPath = batDir + File.separator + fileNameTem + ".mlb";

		File txttempfile = new File(batDir, fileNameTem + ".bat");
		if (txttempfile.exists()) {
			txttempfile.delete();
		}

		BufferedWriter bw;
		try {
			txttempfile.createNewFile();
			bw = new BufferedWriter(new FileWriter(batFile, false));
			bw.write(bat);
			bw.flush();
			bw.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		BalySqlloadThread thread = new BalySqlloadThread();
		try {
			thread.runcmd(batFile);
		} catch (Exception e1) {
			e1.printStackTrace();
		}

		BufferedReader br = null;
		try {
			br = new BufferedReader(new InputStreamReader(new FileInputStream(fixPath)));
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		}
		readerMlbFile(mlbPath);

		String lineData = null;
		StringBuilder sb = new StringBuilder();

		String value = null;
		List<String> list = getFieldSign();
		String regex = null;
		boolean mainFlag = false;
		boolean pnFlag1 = false;

		String sqlmain = "INSERT INTO "
				+ CLT_GPS_ENSURE_POS_MAIN
				+ "(\"OMCID\",\"COLLECTTIME\",\"STAMPTIME\","
				+ "\"LOCAL_TIME\","
				+ "\"GPS_TIME\", \"GPS_SOURCE\", \"GPS_MIN\", \"GPS_TYPE\", \"GPS_SESSION\", \"APPLICATION\", \"POSITION_ENGINE\", \"GPS_RESULT\", \"GPS\", \"AFLT\", \"EFLT\", \"ALTITUDE\",\"INIT_LATITUDE\",\"INIT_LONGITUDE\",\"INIT_ALT_M\",\"INIT_SOURCE\",\"INIT_HEPE_M\", \"RAW_LATITUDE\", \"RAW_LONGITUDE\",\"RAW_ALT_M\",\"RAW_SOURCE\", \"RAW_HEPE_M\", \"OUTPUT_LATITUDE\", \"OUTPUT_LONGITUDE\",\"OUTPUT_ALT_M\",\"OUTPUT_SOURCE\",\"OUTPUT_HEPE_M\", \"MOB_SYS_T_OFFSET\", \"SRV_BS\", \"NID\", \"SID\", \"BAND_FREQ\", \"REF_PN\", "
				+ "\"PWR\",BS_FOUND_MATCHES,BS_PASS,BS_CELL,BS_SCTR,BS_PN,BS_FREQ,BS_BAND,BS_SID,BS_NID,BS_SWNO,BS_VCNTY,PN_LOOKUP_PASS_NUM,PN_LOOKUP_FAIL_NUM,PN_LOOKUP_PASS_DETAIL,PN_LOOKUP_FAIL_DETAIL)VALUES(";

		StringBuilder mainsb = new StringBuilder();
		int omcId = collectObjInfo.getDevInfo().getOmcID();// 1;
		String stamptime = util.Util.getDateString(collectObjInfo.getLastCollectTime());
		// //
		// "2010-03-21 08:08:00";

		mainSqlList = new ArrayList<String>();
		pn1SqlList = new ArrayList<String>();
		pn2SqlList = new ArrayList<String>();
		Date begin = new Date();
		String serving_bs = null;
		String localtime = null;
		String sourcetime = null;
		String minvalue = "0";

		// 一行一行读取
		try {
			while ((lineData = br.readLine()) != null) {
				try {
					lineData = lineData.trim();
					sb.append(lineData + "\r\n");

					// 主表信息处理
					if (lineData.contains("PWR:")) {
						localtime = null;
						mainsb.append(omcId).append(",");
						mainsb.append("sysdate").append(",");
						mainsb.append("to_date('" + stamptime + "','yyyy-MM-dd HH24:mi:ss'),");
						for (int i = 0; i < list.size(); i++) {
							regex = list.get(i);
							value = null;
							value = findByRegex(sb.toString(), regex, 1);

							if (regex.contains(time)) {
								value = value.trim();
								localtime = localTimeMap.get(value);

								/*
								 * if ( localtime == null ) { mainFlag = false; pnFlag1 = false; exitMainFlag = true; continue; }
								 */
								try {
									localtime = localtime.replace("/", "-").trim();
								} catch (Exception e) {
									log.error("value : " + value + " " + localtime);
								}
								if (localtime.lastIndexOf(".") != -1) {
									localtime = localtime.substring(0, localtime.lastIndexOf("."));
								}
								localtime = getDate(localtime);
								mainsb.append("to_date('" + localtime + "','yyyy-MM-dd HH24:mi:ss'),");

								sourcetime = value.replace(",", "").trim();;
								mainsb.append(sourcetime + ",");

							} else if (regex.contains(source)) {
								mainsb.append(getSqlValue(value));
							} else if (regex.contains(min)) {
								minvalue = value;
								mainsb.append(value + ",");
							}

							else if (regex.contains(type)) {
								mainsb.append(getSqlValue(value));
							} else if (regex.contains(session)) {
								mainsb.append(value + ",");
							}

							else if (regex.contains(application)) {
								mainsb.append(value + ",");
							} else if (regex.contains(position_engine)) {
								mainsb.append(getSqlValue(value));
							}

							else if (regex.contains(result)) {
								mainsb.append(getSqlValue(value));
							} else if (regex.contains(gps)) {
								mainsb.append(value + ",");
							} else if (regex.contains(aflt)) {
								mainsb.append(value + ",");
							} else if (regex.contains(eflt)) {
								mainsb.append(value + ",");
							} else if (regex.contains(altitude)) {
								mainsb.append(value + ",");
							} else if (regex.contains(init_latitude)) {
								mainsb.append(value + ",");
							} else if (regex.contains(init_longitude)) {
								mainsb.append(value + ",");
							} else if (regex.contains(init_alt_m)) {
								mainsb.append(value + ",");
							}

							else if (regex.contains(init_source))// source
							{
								mainsb.append(getSqlValue(value));
							}

							else if (regex.contains(init_hepe_m)) {
								mainsb.append(value + ",");
							}

							else if (regex.contains(raw_latitude)) {
								mainsb.append(value + ",");
							} else if (regex.contains(raw_longitude)) {
								mainsb.append(value + ",");
							} else if (regex.contains(raw_alt_m)) {
								mainsb.append(value + ",");
							}

							else if (regex.contains(raw_source))// source
							{
								mainsb.append(getSqlValue(value));
							} else if (regex.contains(raw_hepe_m)) {
								mainsb.append(value + ",");
							}

							else if (regex.contains(output_latitude)) {
								mainsb.append(value + ",");
							} else if (regex.contains(output_longitude)) {
								mainsb.append(value + ",");
							} else if (regex.contains(output_alt_m)) {
								mainsb.append(value + ",");
							}

							else if (regex.contains(output_source))// source
							{
								mainsb.append(getSqlValue(value));
							} else if (regex.contains(output_hepe_m)) {
								mainsb.append(value + ",");
							} else if (regex.contains(mob_sys_t_offset)) {
								mainsb.append(value + ",");
							} else if (regex.contains(srv_bs)) {
								mainsb.append(getSqlValue(value));
							} else if (regex.contains(nid)) {
								mainsb.append(value + ",");
							} else if (regex.contains(sid)) {
								mainsb.append(value + ",");
							} else if (regex.contains(band_freq)) {
								mainsb.append(getSqlValue(value));
							} else if (regex.contains(ref_pn)) {
								mainsb.append(value + ",");
							}

							else if (regex.contains(pwr)) {
								mainsb.append(value + ",");
							}
						}
						// if ( exitMainFlag )
						// {
						// mainFlag = false;
						// pnFlag1 = false;
						// exitMainFlag = false;
						// sb.delete(0, sb.length());
						// continue;
						// }
						mainFlag = true;
						sb.delete(0, sb.length());

						continue;
					}
					// PN1表处理
					if (mainFlag) {
						// String regEx = "Serving BS.+\r\nServing BS.+\r\n";
						String regEx2 = "Serving BS   -- Found.+\r\n(Serving BS\r\n)?";
						if (lineData.contains("Serving BS   -- Found") && (serving_bs = findByRegex(sb.toString(), regEx2, 0)) != null) {

							String[] bs = serving_bs.split("\r\n");
							String bsline = bs[0];

							// System.out.println(bsline);
							String bsfoundmatch = bsline.substring(bsline.indexOf("Found") + 5, bsline.indexOf("matches")).trim();
							String bspass = null;
							if (bsline.contains("(pass:")) {
								bspass = bsline.substring(bsline.indexOf("(pass:") + 6, bsline.indexOf(")")).trim();
							} else {
								if (bsline.contains("(FAIL:")) {
									bspass = bsline.substring(bsline.indexOf("(FAIL:") + 6, bsline.indexOf(")")).trim();
								}
							}
							String bscell = bsline.substring(bsline.indexOf("Cell") + 4, bsline.indexOf("Sctr")).trim();
							String bsSctr = bsline.substring(bsline.indexOf("Sctr") + 4, bsline.indexOf("PN")).trim();
							String bspn = bsline.substring(bsline.indexOf("PN") + 2, bsline.indexOf("Freq")).trim();
							String bsFreq = bsline.substring(bsline.indexOf("Freq") + 4, bsline.indexOf("Band")).trim();

							String bsBand = bsline.substring(bsline.indexOf("Band") + 4, bsline.indexOf("SID")).trim();
							String bsSID = bsline.substring(bsline.indexOf("SID") + 3, bsline.indexOf("NID")).trim();
							String bsNID = bsline.substring(bsline.indexOf("NID") + 3, bsline.indexOf("SwNo")).trim();
							String bsSwNo = bsline.substring(bsline.indexOf("SwNo") + 4, bsline.indexOf("VCNTY")).trim();
							String bsVCNTY = bsline.substring(bsline.indexOf("VCNTY") + 5).trim();
							String[] matchResult = sb.toString().trim().split("\r\n\r\n");

							mainsb.append(bsfoundmatch + ",");
							mainsb.append(bspass + ",");
							mainsb.append(bscell + ",");
							mainsb.append(bsSctr + ",");
							mainsb.append(bspn + ",");
							mainsb.append(bsFreq + ",");
							mainsb.append(bsBand + ",");
							mainsb.append(bsSID + ",");
							mainsb.append(bsNID + ",");
							mainsb.append(bsSwNo + ",");
							mainsb.append(bsVCNTY + ",");

							String pn1Data = null;
							if (matchResult.length > 0) {
								pn1Data = matchResult[0];
							}
							if (pn1Data != null) {
								String[] pn1Datas = pn1Data.split("\r\n");
								String tableHead = pn1Datas[0];
								String[] field = tableHead.split("\\s+");
								Map<Integer, String> valueMap = null;
								valueMap = new HashMap<Integer, String>();
								StringBuilder tableHeader = new StringBuilder();
								StringBuilder tableColumn = new StringBuilder();

								tableHeader.append("OMCID").append(",");
								tableHeader.append("COLLECTTIME").append(",");
								tableHeader.append("STAMPTIME").append(",");

								int fieldLeng = field.length;
								StringBuilder sqltemp = new StringBuilder("INSERT INTO ");
								sqltemp.append(CLT_GPS_ENSURE_POS_PN1);

								tableHeader.append("LOCAL_TIME").append(",");
								tableHeader.append("GPS_TIME").append(",");
								tableHeader.append("GPS_MIN").append(",");

								StringBuilder tempsb = new StringBuilder();
								for (int i = 0; i < fieldLeng; i++) {
									if (field[i] != null) {
										field[i] = field[i].toUpperCase();
										if (field[i].contains(".") || field[i].contains("/")) {
											if (field[i].lastIndexOf(".") == field[i].length() - 1) {
												field[i] = field[i].substring(0, field[i].length() - 1);
											}
											field[i] = field[i].replace(".", "_");
											field[i] = field[i].replace("/", "_");
										}
										valueMap.put(i, field[i]);

										if (i < fieldLeng)// fieldLeng - 1
										{
											tempsb.append(field[i] + ",");
										}
										// else
										// {
										// tableHeader.append(field[i]);
										// }
									}
								}
								// OMCID,COLLECTTIME,STAMPTIME,LOCAL_TIME,GPS_TIME,GPS_MIN,PN,TYPE,ELEV,AZIM,RNG_EST,SD_PP,RMSE,RES_PP,RES_TRUTH,PPHASE,EC_I0,EIP,BAND,FREQ,BSID,EXT_ID,
								String tempField = tempsb.toString().replace("TYPE", "GPS_TYPE");
								tempsb.delete(0, tempsb.length());
								tableHeader.append(tempField);
								tableHeader.append("P_F");
								sqltemp.append("(").append(tableHeader).append(")values (");
								String headTemp = sqltemp.toString();

								String sql = null;
								for (int i = 1; i < pn1Datas.length; i++) {
									String line = pn1Datas[i].trim();
									if (line == null || "".equals(line.trim()))
										continue;

									tableColumn.append(omcId).append(",");
									tableColumn.append("sysdate").append(",");
									tableColumn.append("to_date('" + stamptime + "','yyyy-MM-dd HH24:mi:ss'),");

									tableColumn.append("to_date('" + localtime + "','yyyy-MM-dd HH24:mi:ss'),");

									tableColumn.append(sourcetime).append(",");
									// tableColumn.append("to_date('" +
									// sourcetime
									// + "','yyyy-MM-dd HH24:mi:ss'),");
									tableColumn.append(minvalue + ",");

									String[] data = line.split("\\s+");
									String pn = null;
									boolean pfFlag = false;
									int starCount = 0;
									for (int j = 0; j < data.length; j++) {
										pn = data[j];
										Set<Entry<Integer, String>> setEntry = valueMap.entrySet();
										Iterator<Entry<Integer, String>> itt = setEntry.iterator();

										while (itt.hasNext()) {
											Entry<Integer, String> obj = itt.next();
											if (j == obj.getKey()) {
												if (pn.indexOf("*") != -1 && pn.charAt(0) != '*') {
													pn = pn.substring(0, pn.indexOf("*"));
													pfFlag = true;
													starCount++;
												}
												int star = 0;
												if (pn.charAt(0) == '*') {
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
												if (j < setEntry.size()) {

													tableColumn.append(pn == null ? null + "," : getSqlValue(pn));
												}
											}
										}
									}
									if (pfFlag) {
										tableColumn.append("'PASS'");
									} else {
										tableColumn.append("'FAIL'");
									}

									sql = headTemp + tableColumn.toString() + ")";
									pn1SqlList.add(sql);
									executeBatch(pn1SqlList);
									pn1Count = pn1Count + pn1SqlList.size();
									pn1SqlList.clear();
									tableColumn.delete(0, tableColumn.length());
								}
								pnFlag1 = true;
								sb.delete(0, sb.length());
								// 拼接main主表sql ，当pn2没有数据时，对main的处理方式
								String tempLineData = br.readLine().trim();
								if (tempLineData.equals("")) {
									mainFlag = false;
									pnFlag1 = false;
									pfpass = 0;
									pffail = 0;

									mainsb.append(pfpass + ",");
									mainsb.append(pffail + ",'','')");
									mainSqlList.add(sqlmain + mainsb.toString());
									executeBatch(mainSqlList);
									mainCount = mainCount + mainSqlList.size();
									mainSqlList.clear();
									mainsb.delete(0, mainsb.length());
									continue;
								}
								continue;
							}
						}
						// PN信息表2处理
						if (pnFlag1 && sb.toString().trim().length() > 0 && sb.toString().contains("\r\n\r\n")) {
							String[] pn1temp = sb.toString().trim().split("\r\n");
							String fields = "TYPE,MATCH,CAND,P_F,LAT,LON,HT,HEPE,PRSUM,PCOV,PPHASE,PN,FREQ,BAND,BSID,EXTBSID,SID,NID,SWNO,RNG,AZ,MAR,CTR,ORI,OPN,PR,FLC,FLCU";
							String sqltem = "INSERT INTO "
									+ CLT_GPS_ENSURE_POS_PN2
									+ "(OMCID,COLLECTTIME,STAMPTIME,"
									+ "\"LOCAL_TIME\","
									+ "\"GPS_TIME\", \"GPS_MIN\","
									+ "GPS_TYPE,MATCH,CAND,P_F,LAT,LON,HT,HEPE,PRSUM,PCOV,PPHASE,PN,FREQ,BAND,BSID,EXTBSID,SID,NID,SWNO,RNG,AZ,MAR,CTR,ORI,OPN,PR,FLC,FLCU)VALUES(";

							pn2SqlList = getSql(pn1temp, sqltem, fields, localtime, sourcetime, minvalue);

							executeBatch(pn2SqlList);
							pn2Count = pn2Count + pn2SqlList.size();
							pn2SqlList.clear();
							sb.delete(0, sb.length());
							// sb.append(lineData);
							mainFlag = false;
							pnFlag1 = false;

							// 拼接main主表sql
							mainsb.append(pfpass + ",");
							mainsb.append(pffail + ",");

							if (pnLookupPassDetail.length() > 0) {
								pnLookupPassDetail.deleteCharAt(pnLookupPassDetail.length() - 1);
								mainsb.append("'" + pnLookupPassDetail).append("',");
							} else {
								mainsb.append("''").append(",");
							}
							if (pnLookupFailDetail.length() > 0) {
								pnLookupFailDetail.deleteCharAt(pnLookupFailDetail.length() - 1);
								mainsb.append("'" + pnLookupFailDetail + "'");
							} else {
								mainsb.append("''");
							}
							mainsb.append(")");

							mainSqlList.add(sqlmain + mainsb.toString());
							executeBatch(mainSqlList);
							mainCount = mainCount + mainSqlList.size();
							mainSqlList.clear();
							mainsb.delete(0, mainsb.length());

							pnLookupPassDetail.delete(0, pnLookupPassDetail.length());
							pnLookupFailDetail.delete(0, pnLookupFailDetail.length());

							pfpass = 0;
							pffail = 0;
							continue;
						}
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			if (mainCount == 0 && pn1Count == 0 && pn2Count == 0)
				return;
			// 统计个表入库数量
			collectCount();

			Date end = new Date();
			log.info(collectObjInfo + " : 解析并分发：" + fileName + " 文件成功，一共消耗时间：" + (int) (end.getTime() - begin.getTime()));

			FTPConfig cfg = FTPConfig.getFTPConfig(collectObjInfo.getTaskID());
			boolean isDel = SystemConfig.getInstance().isDeleteWhenOff();
			if (cfg != null)
				isDel = cfg.isDelete();
			// 删除.txt ,.csv ,.mlb ,.bat,.fix文件
			if (isDel) {
				log.debug(collectObjInfo + ": 开始删除GPS 临时文件.");
				String temFile = batDir + File.separator + fileNameTem;
				deleteGpsLog(temFile);
			}

			localTimeMap.clear();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null)
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}

		}

	}

	// 统计各表入库数量
	private void collectCount() {

		int omcId = collectObjInfo.getDevInfo().getOmcID();
		long lastTime = collectObjInfo.getLastCollectTime().getTime();
		dbLogger.log(omcId, CLT_GPS_ENSURE_POS_MAIN, lastTime, mainCount, collectObjInfo.getTaskID());
		log.info(collectObjInfo.getTaskID() + " 表: " + CLT_GPS_ENSURE_POS_MAIN + " 入库了" + mainCount + "条数据");
		dbLogger.log(omcId, CLT_GPS_ENSURE_POS_PN1, lastTime, pn1Count, collectObjInfo.getTaskID());
		log.info(collectObjInfo.getTaskID() + " 表: " + CLT_GPS_ENSURE_POS_PN1 + " 入库了" + pn1Count + "条数据");

		dbLogger.log(omcId, CLT_GPS_ENSURE_POS_PN2, lastTime, pn2Count, collectObjInfo.getTaskID());
		log.info(collectObjInfo.getTaskID() + " 表: " + CLT_GPS_ENSURE_POS_PN2 + " 入库了" + pn2Count + "条数据");

	}

	public String getSqlValue(String value) {
		if (value == null || "".equals(value.trim()))
			return null + ",";
		value = "'" + value + "',";
		return value;
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
	public List<String> getSql(String[] pn1Datas, String sql, String fields, String localtime, String sourcetime, String min) {
		int omcId = collectObjInfo.getDevInfo().getOmcID(); // 1;
		String stamptime = util.Util.getDateString(collectObjInfo.getLastCollectTime());
		// //
		// "2010-03-21 08:08:00";
		StringBuilder sbtemp = new StringBuilder();
		int pnFlag = -1;// -1:null,0:fail,1:pass

		for (int i = 1; i < pn1Datas.length; i++) {
			String line = pn1Datas[i];
			if (line == null || line.equals(""))
				continue;
			sbtemp.append(omcId).append(",");
			sbtemp.append("sysdate").append(",");
			sbtemp.append("to_date('" + stamptime + "','yyyy-MM-dd HH24:mi:ss'),");

			sbtemp.append("to_date('" + localtime + "','yyyy-MM-dd HH24:mi:ss'),");

			sbtemp.append(sourcetime).append(",");

			// sbtemp.append("to_date('" + sourcetime
			// + "','yyyy-MM-dd HH24:mi:ss'),");
			sbtemp.append(min + ",");

			pn2_type = line.substring(0, 5);
			sbtemp.append(getSqlValue(pn2_type));
			match = line.substring(6, 10);
			sbtemp.append(getSqlValue(match));
			cand = line.substring(11, 14);
			sbtemp.append(getSqlValue(cand));
			p_f = line.substring(15, 19);
			if (p_f != null) {
				if (p_f.trim().equalsIgnoreCase("pass")) {
					pnFlag = 1;
					pfpass++;
				} else if (p_f.equalsIgnoreCase("FAIL")) {
					pnFlag = 0;
					pffail++;
				}
			}
			sbtemp.append(getSqlValue(p_f));
			lat = line.substring(20, 29);
			sbtemp.append(getSqlValue(lat));
			lon = line.substring(30, 40);
			sbtemp.append(getSqlValue(lon));
			ht = line.substring(41, 45);
			sbtemp.append(getSqlValue(ht));
			hepe = line.substring(46, 51);
			sbtemp.append(getSqlValue(hepe));
			prsum = line.substring(52, 57);
			sbtemp.append(getSqlValue(prsum));
			pcov = line.substring(58, 63);
			sbtemp.append(getSqlValue(pcov));
			pphase = line.substring(64, 71);
			sbtemp.append(getSqlValue(pphase));
			pn = line.substring(72, 75);
			sbtemp.append(getSqlValue(pn));

			// 保存pn pass or fail 失败信息
			pn = (pn == null) ? null : pn.trim();
			if (pnFlag == 1)
				pnLookupPassDetail.append(pn).append(pnSplit);
			else if (pnFlag == 0)
				pnLookupFailDetail.append(pn).append(pnSplit);
			pnFlag = -1;
			//

			freq = line.substring(76, 80);
			sbtemp.append(getSqlValue(freq));

			if (line.length() < 85) {
				band = line.substring(81, 84);
				sbtemp.append(band);
				sbtemp.append("," + appendNull(fields.substring(fields.indexOf("band".toUpperCase()) + 5).split(",").length));
				pn2SqlList.add(sql + sbtemp.toString() + ")");
				sbtemp.delete(0, sbtemp.length());
				clear();
				continue;
			}
			band = line.substring(81, 84);
			sbtemp.append(getSqlValue(band));
			bsid = line.substring(85, 90);
			sbtemp.append(getSqlValue(bsid));
			extbsid = line.substring(91, 101);
			sbtemp.append(getSqlValue(extbsid));
			pn2_sid = line.substring(102, 107);
			sbtemp.append(getSqlValue(pn2_sid));
			pn2_nid = line.substring(108, 113);

			sbtemp.append(getSqlValue(pn2_nid));
			// System.out.println(line);
			swno = line.substring(114, 117);
			sbtemp.append(getSqlValue(swno));

			rng = line.substring(118, 123);
			sbtemp.append(getSqlValue(rng));

			if (line.length() < 133)//
			{
				az = line.substring(124, 127);
				sbtemp.append(az);

				sbtemp.append("," + appendNull(fields.substring(fields.indexOf("az".toUpperCase()) + 5).split(",").length));
				pn2SqlList.add(sql + sbtemp.toString() + ")");
				sbtemp.delete(0, sbtemp.length());
				clear();
				continue;
			}
			az = line.substring(124, 127);
			sbtemp.append(getSqlValue(az));
			mar = line.substring(128, 133);
			sbtemp.append(getSqlValue(mar));
			ctr = line.substring(134, 139);
			sbtemp.append(getSqlValue(ctr));
			ori = line.substring(140, 143);
			sbtemp.append(getSqlValue(ori));
			opn = line.substring(144, 147);
			sbtemp.append(getSqlValue(opn));
			pr = line.substring(148, 149);
			sbtemp.append(getSqlValue(pr));
			flc = line.substring(150, 154);
			sbtemp.append(getSqlValue(flc));
			flcu = line.substring(155, 159);
			sbtemp.append(flcu);

			pn2SqlList.add(sql + sbtemp.toString() + ")");
			sbtemp.delete(0, sbtemp.length());
			clear();
		}
		return pn2SqlList;
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

	public String appendNull(int n) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < n; i++) {
			if (i < n - 1) {
				sb.append(null + ",");
			} else {
				sb.append(" " + null);
			}
		}
		return sb.toString();
	}

	/* 执行批量sql */
	private void executeBatch(List<String> inserts) throws BatchUpdateException {
		if (inserts == null) {
			return;
		}
		int count = 0;
		Connection connection = DbPool.getConn();
		Statement statement = null;
		String temp = "";
		try {
			connection.setAutoCommit(false);
			statement = connection.createStatement();
			for (String sql : inserts) {
				temp = sql;
				// System.out.println(sql);
				statement.addBatch(sql);
				if (count % batchSize == 0) {
					statement.executeBatch();
				}
				count++;
			}
			statement.executeBatch();
			connection.commit();
		} catch (BatchUpdateException ee) {
			log.error("error sql " + temp);
			log.error(collectObjInfo + ": 定位日志文件插入出现异常", ee);
		} catch (Exception e) {
			if (connection != null) {
				try {
					connection.rollback();
				} catch (SQLException sqlex) {
				}
			}
			log.error(collectObjInfo + " : 插入数据时出现异常.", e);
		} finally {
			try {
				if (statement != null) {
					statement.close();
				}
				if (connection != null) {
					connection.close();
				}
			} catch (Exception e) {
			}
		}
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

	// 1573,398588.560 转换gis定位时间
	public String gisTimeToDate(String gistime) {
		if (gistime == null || gistime.trim().equals(""))
			return null;
		gistime = gistime.trim();
		gistime = gistime.replace(",", "");
		gistime = gistime.replace(".", "");
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		long time = 0;
		try {
			Date d = df.parse(baseOnTime);
			long timeLong = Long.valueOf(gistime);
			time = d.getTime() + timeLong;
		} catch (ParseException e) {
			e.printStackTrace();
		}
		// Date d=new Date()
		// long timeLong = time;// 1573398588l;
		Calendar c = Calendar.getInstance();
		c.setTimeInMillis(time);
		Date date = c.getTime();
		String dateResult = util.Util.getDateString(date);
		return dateResult;
	}

	public String getDate(String times) {
		if (times == null || times.trim().equals(""))
			return null;
		long time = 0;
		long temTime = 0;
		try {
			temTime = Util.getDate1(times).getTime();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		long timeLong = Long.valueOf(temTime);
		time = timeLong + 8 * 60 * 60 * 1000;

		Calendar c = Calendar.getInstance();
		c.setTimeInMillis(time);
		Date date = c.getTime();
		String dateResult = util.Util.getDateString(date);
		return dateResult;
	}

	@Override
	public boolean parseData() {
		String path = this.getFileName();
		try {

			this.parse(path);

		} catch (Exception e) {
			log.error(collectObjInfo + ": " + path + "  文件没有找到", e);
		}
		return true;
	}

	public void deleteGpsLog(String temFile) {
		try {
			File txt = new File(temFile + ".txt");
			File csv = new File(temFile + ".csv");
			File mlb = new File(temFile + ".mlb");
			File b = new File(temFile + ".bat");
			File fix = new File(temFile + ".fix");
			if (txt.exists()) {
				txt.delete();
			}
			if (csv.exists()) {
				csv.delete();
			}
			if (mlb.exists()) {
				mlb.delete();
			}
			if (b.exists()) {
				b.delete();
			}
			if (fix.exists()) {
				fix.delete();
			}
			log.debug(collectObjInfo + ": 删除GPS文件成功 " + "txt: " + txt.getAbsolutePath() + " csv:" + csv.getAbsolutePath() + "  mlb:"
					+ mlb.getAbsolutePath() + " bat:" + b.getAbsolutePath() + " fix:" + fix.getAbsolutePath());
		} catch (Exception e) {
			log.error(collectObjInfo.getTaskID() + ": Delete GPS Log 文件出现异常 ");
		}

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

	public void readerMlbFile(String mlbPath) {
		BufferedReader mlbReader = null;
		try {
			mlbReader = new BufferedReader(new InputStreamReader(new FileInputStream(mlbPath)));
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		}
		String mlbLine = null;
		// StringBuilder mlbSb = new StringBuilder();
		try {
			while ((mlbLine = mlbReader.readLine()) != null) {
				if (mlbLine.trim().equals("") || !mlbLine.trim().substring(0, 3).equals("***")) {
					continue;
				}
				// mlbSb.append(mlbLine + "\n");
				if (mlbLine.length() < 43)
					continue;
				// String result = findByRegex(mlbSb.toString(), localTimeRef,
				// 1);
				String result = mlbLine.substring(3, 43).trim();

				result = result.replace("/ ", "/");
				// String result = "1589,101160.000 2010/06/30 23:59:60.000";
				String datas[] = result.split(" ");
				String key = null;
				String value = null;
				try {
					if (datas.length > 3) {
						key = datas[0] + datas[1];
						value = datas[2] + " " + datas[3];
					} else {
						key = datas[0];
						value = datas[1] + " " + datas[2];
					}
				} catch (Exception e) {
					// log.info(key + " " + value);
				}
				// String time = "2010/06/21 04:05:60.000";

				value = value.replace("/", "-");
				String values[] = value.split(" ");

				String time = values[1];

				String date = values[0];
				String dates[] = date.split("-");
				String year = dates[0];
				String mm = dates[1];
				String dd = dates[2];
				String times[] = time.split(":");
				String hour = times[0];
				String min = times[1];
				String sec = times[2];
				if (sec.equals("60.000")) {
					sec = "00";
					int minC = Integer.valueOf(min);
					if (minC == 59) {
						min = "00";
						if (hour.equals("23")) {
							hour = "00";

							if (Integer.valueOf(dd) == days(Integer.valueOf(year), Integer.valueOf(mm))) {
								dd = "01";
								if ((Integer.valueOf(mm) + 1) <= 12) {
									mm = String.valueOf((Integer.valueOf(mm) + 1));
								} else {
									mm = "01";
									year = String.valueOf((Integer.valueOf(year) + 1));
								}
							}
						} else {
							hour = String.valueOf(Integer.valueOf(hour) + 1);
						}
					} else {
						min = String.valueOf(++minC);
					}
				}

				value = year + "-" + mm + "-" + dd + " " + hour + ":" + min + ":" + sec;
				// System.out.println(value);
				if (!localTimeMap.containsKey(key))
					localTimeMap.put(key, value);
				// mlbSb.delete(0, mlbSb.length());
			}

		} catch (IOException e1) {
			e1.printStackTrace();
		} finally {
			if (mlbReader != null)
				try {
					mlbReader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
		}
	}

	public static int days(int year, int month) {
		int days = 0;
		if (month != 2) {
			switch (month) {
				case 1 :
				case 3 :
				case 5 :
				case 7 :
				case 8 :
				case 10 :
				case 12 :
					days = 31;
					break;
				case 4 :
				case 6 :
				case 9 :
				case 11 :
					days = 30;

			}
		} else {

			if (year % 4 == 0 && year % 100 != 0 || year % 400 == 0)
				days = 29;
			else
				days = 28;
		}
		return days;
	}

	public static void main(String[] args) throws BatchUpdateException {
		Date date = new Date();
		CV1BinInsert pe = new CV1BinInsert();
		String fileName = "D:\\20101010\\txtfile\\jiangsu\\LSL_2010Dec1_JS_2354_220.bin.zip";
		CollectObjInfo obj = new CollectObjInfo(755123);
		DevInfo dev = new DevInfo();
		dev.setOmcID(111);
		obj.setDevInfo(dev);
		obj.setLastCollectTime(new Timestamp(new Date().getTime()));
		pe.collectObjInfo = obj;
		pe.parse(fileName);

		// String gistime = "1573,398588.560";
		// // 158492049360
		System.out.println(System.currentTimeMillis() - date.getTime());
		System.out.println(pe.getDate("2010-07-01 00:00:00"));
	}
}
