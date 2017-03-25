package parser;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

import task.CollectObjInfo;
import task.RegatherObjInfo;
import templet.LineTempletP;
import util.AutoCharset;
import util.CommonDB;
import util.Util;
import datalog.DataLogInfo;
import framework.ConstDef;

public class LineParser extends Parser {

	// 剩余未解析的字符串
	private String remainingData = "";

	// 计算采集的次数
	private int m_ParseTime = 0;

	private String m_RawColumn_List = "";// 原始的列名

	public LineParser() {
	}

	public LineParser(CollectObjInfo collectInfo) {
		super(collectInfo);
	}

	/**
	 * split函数 思路:逐个字符扫描如果发现成对字符upsplitsign则放入数组内, 如果没有则以参数分隔符作为结束点放入数组, 到最后一个字符时则判断将剩余数据放入数组(如果只有一个字符且为分隔符则标识两个空数据)
	 * 
	 * @param linestr
	 * @param splitsign
	 * @param upsplitsign
	 * @return
	 */
	public static String[] split(String linestr, String splitsign, String upsplitsign) {
		if (upsplitsign == null || upsplitsign.length() == 0)
			return linestr.split(splitsign);
		String[] upsplits = upsplitsign.split(",");
		if (upsplits.length < 2) {
			upsplits = new String[2];
			upsplits[0] = upsplitsign; // 起始标识
			upsplits[1] = upsplitsign; // 结束标识
		}

		ArrayList<String> alist = new ArrayList<String>();
		boolean espeflag = false;
		int espebeginindex = 0;
		boolean beginflag = false;
		int splitbegindex = 0;

		for (int i = 0; i < linestr.length(); i++) {
			if (i == linestr.length() - 1) {
				if (splitsign.equals(linestr.substring(i, i + 1))) {
					alist.add("");
					alist.add("");
				} else {
					alist.add(linestr.substring((espeflag ? espebeginindex : splitbegindex), i + 1));
				}
			} else if (upsplits[0].equals(linestr.substring(i, i + 1)) && espeflag == false) {
				// 找到第一个特殊符合
				espeflag = true;
				espebeginindex = i + 1;
				continue;
			} else if (espeflag == true) {
				// 开始查找第2个特殊符号
				if (upsplits[1].equals(linestr.substring(i, i + 1))) {
					// 特殊符号成对,重置成对标识
					alist.add(linestr.substring(espebeginindex, i));
					espeflag = false;
					i = i + 1;
					splitbegindex = i + 1;
				}
				continue;
			} else if (splitsign.equals(linestr.substring(i, i + 1)) && beginflag == false) {
				beginflag = true;
				alist.add(linestr.substring(splitbegindex, i));
				splitbegindex = i + 1;
			} else if (splitsign.equals(linestr.substring(i, i + 1)) && beginflag == true) {
				alist.add(linestr.substring(splitbegindex, i));
				splitbegindex = i + 1;
			}
		}
		String[] rets = alist.toArray(new String[0]);
		return rets;
	}

	public static boolean testData(char[] chData, int iLen) {
		String oddstr = "";
		oddstr += new String(chData, 0, iLen);

		boolean bLastCharN = false;// 最后一个字符是\n
		if (oddstr.charAt(oddstr.length() - 1) == '\n')
			bLastCharN = true;

		// 分行
		String[] strzRowData = oddstr.split("\n");

		// 没有数据
		if (strzRowData.length == 0)
			return true;

		// 特殊标记表示达到最后一行
		int nRowCount = strzRowData.length - 1;
		oddstr = strzRowData[nRowCount];
		if (oddstr.equals("**FILEEND**"))
			oddstr = "";

		// 如果最后一个字符是\n 下次采集的时候,将是补上\n这个字符
		if (bLastCharN)
			oddstr += "\n";
		FileWriter fw = null;
		try {
			fw = new FileWriter("/mrdata/testddd.txt");

			// 最后一行不解析,与下次数据一起解析
			for (int i = 0; i < nRowCount; ++i) {
				try {
					if (strzRowData[i] == null || strzRowData[i].trim().equals(""))
						continue;
					// String[] testargs=
					// ParseData_Line.split(strzRowData[i],",","{,}");
					// String linstr = new
					// String(strzRowData[i].getBytes("ISO-8859-1"), "GB2312");
					String linstr = AutoCharset.getCorrectString(new String(strzRowData[i].getBytes(), "ISO-8859-1"), new String(
							AutoCharset.ORG_STRING.getBytes(), "ISO-8859-1"));
					fw.write(linstr + ";\n");
					// splitlist.add(testargs);

					// String[] ddd=strzRowData[i].split(",");
				} catch (Exception ex) {
					// 导入某行出错，写入日志
					// Log.AddLog(ConstDef.COLLECT_LOGTYPE_ERROR,
					// "第"+m_ParseTime+"分析出错:当前第"+String.valueOf(i+1)+"行.\n"+Err.getMessage());
					ex.printStackTrace();
				}
			}
			fw.close();
		} catch (Exception e) {

		}
		return true;
	}

	public static void main(String[] args) {
		try {
			FileReader reader = new FileReader("/mrdata/test.csv");
			char[] buff = new char[65536];

			int iLen = 0;
			while ((iLen = reader.read(buff)) > 0) {
				testData(buff, iLen);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public boolean parseData() throws Exception {
		FileReader reader = null;

		try {
			String logStr = this + ": starting parse file : " + fileName;
			log.debug(logStr);
			collectObjInfo.log(DataLogInfo.STATUS_PARSE, logStr);

			reader = new FileReader(fileName);
			char[] buff = new char[65536];

			int iLen = 0;
			while ((iLen = reader.read(buff)) > 0) {
				BuildData(buff, iLen);
			}

			// 加结束标记
			String strEnd = "\n**FILEEND**";
			BuildData(strEnd.toCharArray(), strEnd.length());
		} finally {
			try {
				if (reader != null)
					reader.close();
			} catch (Exception e) {
			}
		}

		return true;
	}

	public boolean BuildData(char[] chData, int iLen) {
		boolean bReturn = true;

		remainingData += new String(chData, 0, iLen);

		String logStr = null;

		// 解析的次数
		if (++m_ParseTime % 100 == 0) {
			logStr = this + ": " + collectObjInfo.getDescribe() + " parse time:" + m_ParseTime;
			log.debug(logStr);
			collectObjInfo.log(DataLogInfo.STATUS_PARSE, logStr);
		}
		boolean bLastCharN = false;// 最后一个字符是\n
		if (remainingData.charAt(remainingData.length() - 1) == '\n')
			bLastCharN = true;

		// 分行
		String[] strzRowData = remainingData.split("\n");

		// 没有数据
		if (strzRowData.length == 0)
			return true;

		// 特殊标记表示达到最后一行
		int nRowCount = strzRowData.length - 1;
		remainingData = strzRowData[nRowCount];
		if (remainingData.equals("**FILEEND**"))
			remainingData = "";

		// 如果最后一个字符是\n 下次采集的时候,将是补上\n这个字符
		if (bLastCharN)
			remainingData += "\n";

		try {
			// 最后一行不解析,与下次数据一起解析
			for (int i = 0; i < nRowCount; ++i) {
				if (Util.isNull(strzRowData[i]))
					continue;
				ParseLineData(strzRowData[i]);
			}
		} catch (Exception e) {
			bReturn = false;
			logStr = this + ": Cause:";
			log.error(logStr, e);
			collectObjInfo.log(DataLogInfo.STATUS_PARSE, logStr, e);
		}

		return bReturn;
	}

	/*
	 * 按照行来分析的函数
	 */
	public void ParseLineData(String strOldRow) {
		if (strOldRow == null || Util.isNull(strOldRow))
			return;
		boolean isExistReservedKeyWord = false;// 是否存在保留的关键字
		// 扫描类型为0 遇到不需要保留的关键字。就退出
		// 扫描类型为１，遇到保留的关键字．就采集．
		int nSubTmpIndex = 0;
		int nColumnIndex = 0;

		// 行模板
		LineTempletP templet = (LineTempletP) (collectObjInfo.getParseTemplet());

		// 子模板
		LineTempletP.SubTemplet subTemp;

		// 根据扫描类型获取子模板的索引 nSubTmpIndex
		switch (templet.nScanType) {
			case 0 :
				for (int j = 0; j < templet.unReserved.size(); ++j) {
					if (strOldRow.indexOf(templet.unReserved.get(j)) == 0 || strOldRow.trim().indexOf(templet.unReserved.get(j).trim()) == 0)
						return;
				}
				break;
			case 1 :
				for (int i = 0; i < templet.m_nTemplet.size(); ++i) {
					subTemp = templet.m_nTemplet.get(i);
					// 开始位置存在关键字，则保留该行
					if (strOldRow.indexOf(subTemp.m_strLineHeadSign) == 0) {
						isExistReservedKeyWord = true;
						nSubTmpIndex = i;
						nColumnIndex = 1;
						break;
					}
				}
				if (!isExistReservedKeyWord)
					return;
				break;
			case 2 : // 根据文件名得到表的类型
				String strShortFileName = fileName.substring(fileName.lastIndexOf(File.separatorChar) + 1);
				for (int i = 0; i < templet.m_nTemplet.size(); ++i) {
					subTemp = templet.m_nTemplet.get(i);
					// String strFileName = subTemp.m_strFileName.toUpperCase();
					String strFileName = ConstDef.ParseFilePath(subTemp.m_strFileName, collectObjInfo.getLastCollectTime());
					if (subTemp.m_nFileNameCompare == 0) {
						if (logicEquals(strShortFileName, strFileName)) {
							nSubTmpIndex = i;
							// add by litp 2010-05-27，原因：考虑到补采时数据源需要知道对应模板中在索引号
							if (CommonDB.isReAdoptObj(collectObjInfo)) {
								RegatherObjInfo rTask = (RegatherObjInfo) collectObjInfo;
								rTask.addTableIndex(i);
							}
							// add end
							break;
						}
					} else if (subTemp.m_nFileNameCompare == 1) {
						if (strShortFileName.indexOf(strFileName) == 0) {
							nSubTmpIndex = i;
							collectObjInfo.setActiveTableIndex(i);
							break;
						}
					}
				}
				// 删除非保留字段
				for (int j = 0; j < templet.unReserved.size(); ++j) {
					if (strOldRow.indexOf(templet.unReserved.get(j)) == 0)
						return;
				}
				break;
		}

		// 获取属于哪个子模板类型nSubTmpIndex为他的索引号
		subTemp = templet.m_nTemplet.get(nSubTmpIndex);

		// 组成新行需要的字符串
		StringBuffer strNewRow = new StringBuffer();
		String strValue = "";

		if (subTemp.m_RawColumnList != null && !subTemp.m_RawColumnList.equals("") && strOldRow.indexOf(subTemp.m_RawColumnList) == 0) {
			if (!"".equals(subTemp.m_ColumnListAppend))
				m_RawColumn_List = subTemp.m_ColumnListAppend + subTemp.m_strNewFieldSplitSign + strOldRow;
			strOldRow = m_RawColumn_List;
			strNewRow.append("OMCID" + subTemp.m_strNewFieldSplitSign + "COLLECTTIME" + subTemp.m_strNewFieldSplitSign + "STAMPTIME"
					+ subTemp.m_strNewFieldSplitSign);
		} else {
			// 每行添加 OMCID 并添加新的分隔符号
			strNewRow.append(collectObjInfo.getDevInfo().getOmcID());
			strNewRow.append(subTemp.m_strNewFieldSplitSign);
			// 添加当前时间 格式YYYY-MM-DD HH24:MI:SS 并添加新的分隔符号
			Date now = new Date();
			SimpleDateFormat spformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			String strTime = spformat.format(now);
			strNewRow.append(strTime + subTemp.m_strNewFieldSplitSign);

			// 添加最后下载时间
			strTime = spformat.format(collectObjInfo.getLastCollectTime());
			// TimeStamp
			strNewRow.append(strTime + subTemp.m_strNewFieldSplitSign);

			// 添加Ciyt_ID ---by Xumg
			/*
			 * strNewRow.append( m_TaskInfo.get_DevInfo().get_CityID() ); strNewRow.append( subTemp.m_strNewFieldSplitSign);
			 */
		}

		try {
			switch (subTemp.m_nParseType) {
				case ConstDef.COLLECT_LINE_PARSE_SPLIT :
					strValue = ParseRowBySplit(subTemp, nColumnIndex, strOldRow);
					break;
				case ConstDef.COLLECT_LINE_PARSE_BITPOS :
					strValue = ParseRowByPosition(subTemp, strOldRow);
					break;
				case ConstDef.COLLECT_LINE_PARSE_RAW :
					strValue = ParsrRowByRaw(subTemp, strOldRow);
					break;
				case ConstDef.COLLECT_LINE_PARSE_FREEDOM :
					strValue = strOldRow.replace(subTemp.m_strFieldSplitSign, subTemp.m_strNewFieldSplitSign);
					if (strValue.endsWith(subTemp.m_strNewFieldSplitSign))
						strValue = strValue.concat(subTemp.m_strNewFieldSplitSign);
					break;
			}
		} catch (Exception e) {
			String str = this + " : error when parsing data. templet name : " + templet.tmpName + " data:" + strOldRow;
			log.error(str, e);
			collectObjInfo.log(DataLogInfo.STATUS_PARSE, str, e);
			return;
		}

		strNewRow.append(strValue + "\n");
		distribute.DistributeData(strNewRow.toString().getBytes(), subTemp.m_nLineHeadType);
	}

	/**
	 * 判断两个字符串是否是逻辑意义上的相等。一个字符串是有通配符的，另一个是没有通配符的。 例如，"*_Adjacent_cell_handover_01Jul2010_*.csv"
	 * 可以匹配到"4_Adjacent_cell_handover_01Jul2010_0433.csv"，于是它们相等。 如果没有通配符，就按String.equals()方法来判断。
	 * 
	 * @param shortFileName
	 *            实际的文件名
	 * @param fileName
	 *            解析模板中，<FILENAME>中配的文件名
	 * @return 是否相等
	 */
	private boolean logicEquals(final String shortFileName, final String fileName) {
		// 不包含通配符的情况下，当作普通的String.equals()方法处理
		if (!fileName.contains("*") && !fileName.contains("?")) {
			return shortFileName.equals(fileName);
		}

		String s1 = shortFileName.replaceAll("\\.", ""); // 把.号去掉，因为它在正则表达式中有意义。
		String s2 = fileName.replaceAll("\\.", ""); // 把.号去掉，因为它在正则表达式中有意义。
		s1 = s1.replaceAll("\\+", "");
		s2 = s2.replaceAll("\\+", "");
		s2 = s2.replaceAll("\\*", ".*"); // *换成.*，表示多匹配多个字符
		s2 = s2.replaceAll("\\?", "."); // ?换成.，表示匹配单个字符

		return Pattern.matches(s2, s1); // 通过正则表达式方式判断
	}

	private String ParsrRowByRaw(LineTempletP.SubTemplet subTemp, String strRow) {
		// 就根据原始数据来进行解析
		String[] m_strTemp = strRow.split(subTemp.m_strFieldSplitSign);
		strRow = strRow.replaceAll(subTemp.m_strFieldSplitSign, subTemp.m_strNewFieldSplitSign);
		if (m_strTemp.length < subTemp.m_nColumnCount) {
			int nCount = subTemp.m_nColumnCount - m_strTemp.length;// 当列数不一致时增加附加信息内容
			for (int i = 0; i < nCount; i++)
				strRow += subTemp.m_strNewFieldSplitSign;
		}
		return strRow;
	}

	private static final String[] trimSplit(String row, String split) {
		if (split.equals("\t") || split.equals(" ")) {
			String[] sp = row.split(split);
			List<String> list = new ArrayList<String>();
			int count = 0;
			for (String s : sp) {
				if (Util.isNotNull(s)) {
					s = s.trim();
					if (s.equalsIgnoreCase("n/a") && count != 5)// TO_ACTIVE_AP_IP字段不做空处理，保留原值
																// -- by yuy
																// 2013-06-26
						s = "";
					list.add(s);
					count++;
				}
			}
			return list.toArray(new String[0]);
		} else {
			return row.split(split);
		}
	}

	// 利用Split 来解析内容
	private String ParseRowBySplit(LineTempletP.SubTemplet subTemp, int nColumnIndex, String strRow) {
		// 获得行模板
		LineTempletP templet = (LineTempletP) (collectObjInfo.getParseTemplet());
		// 该行根据子模板规定的关键字，将他分隔成每个数据
		String[] m_strTemp;
		if (subTemp.m_strFieldUpSplitSign == null || subTemp.m_strFieldUpSplitSign.length() == 0)
			m_strTemp = trimSplit(strRow, subTemp.m_strFieldSplitSign);
		else
			m_strTemp = LineParser.split(strRow, subTemp.m_strFieldSplitSign, subTemp.m_strFieldUpSplitSign);

		// 组成新行需要的字符串
		StringBuffer m_TempString = new StringBuffer();
		// 计算列数
		int nCount = 0;
		String nvl = subTemp.nvl;
		for (int k = nColumnIndex; k < m_strTemp.length; k++) {
			// 在模板中定义了字段与数据列的索引的对应关系,
			// 如果这个字段对应的HashMap个数为0的话,表示全部导入，表示没有对应关系
			// HashMap<数据所在列的索引号,字段名称>
			// 当HashMap个数大于0的话.而且数据所在列索引号存在的话,则添加该数据
			// 否则就跳出本次循环
			if (templet.columnMapping.size() > 0) {
				if (!templet.columnMapping.containsKey(k))
					continue;
			}
			// 如果某行有１００个数据，只需要前面５０个，则不需要追加后面的数据
			if (nCount >= subTemp.m_nColumnCount)
				break;

			// 如果当前该数据为空，则添加０
			if (m_strTemp[k] == null || m_strTemp[k].trim().equals("")) {
				// m_TempString.append("0");
				// nvl = subTemp.nvl;
				m_TempString.append(nvl);
			} else {
				// 如果是某些特定类型，进行转换
				try {
					String type = subTemp.m_Filed.get(k).m_type;
					if (type != null && type.equals("DATE")) {
						// 日期格式
						String dateFormat = subTemp.m_Filed.get(k).m_dateFormat;
						SimpleDateFormat format1 = new SimpleDateFormat(dateFormat);
						SimpleDateFormat format2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

						Date date = format1.parse(m_strTemp[k].trim());
						String resultDate = format2.format(date);
						m_TempString.append(resultDate);
					}
				} catch (ParseException e) {
					m_TempString.append("1970-1-1 08:00:00");
				} catch (Exception e) {
				}
				// change on 2010-04-06 将;替换为空格
				// m_TempString.append(m_strTemp[k].trim());
				m_TempString.append(removeNoiseSemicolon(m_strTemp[k].trim()));
			}
			// 最后一个数据后面不需要分隔符号．其他的数据后面都需要.
			if (k < m_strTemp.length - 1 && nCount < subTemp.m_nColumnCount - 1)
				m_TempString.append(subTemp.m_strNewFieldSplitSign);
			nCount++;
		}
		if (nCount < subTemp.m_nColumnCount) {
			for (int k = nCount; k < subTemp.m_nColumnCount; k++) {
				// add
				m_TempString.append(subTemp.m_strNewFieldSplitSign + nvl);
				// m_TempString.append(subTemp.m_strNewFieldSplitSign + "0");
				nCount++;
			}
		}
		return m_TempString.toString();
	}

	// 按位截取来分析
	// 利用起始位置来解析内容
	private String ParseRowByPosition(LineTempletP.SubTemplet subTemp, String strRow) {
		StringBuffer m_TempString = new StringBuffer();
		int len = subTemp.m_Filed.size();
		String nvl = subTemp.nvl;
		for (int i = 0; i < len; i++) {
			LineTempletP.FieldTemplet field = subTemp.m_Filed.get(i);

			String strValue = "";
			if (field.m_nStartPos + field.m_nDataLength > strRow.length())
				strValue = strRow.substring(field.m_nStartPos);
			else
				strValue = strRow.substring(field.m_nStartPos, field.m_nStartPos + field.m_nDataLength);

			if (i < subTemp.m_Filed.size() - 1) {
				if (strValue.trim().equals("")) {
					// m_TempString.append("0" +
					// subTemp.m_strNewFieldSplitSign);
					m_TempString.append(nvl + subTemp.m_strNewFieldSplitSign);
				} else {
					// change on 2010-04-06 将;替换为空格
					// m_TempString.append(strValue.trim()
					// + subTemp.m_strNewFieldSplitSign);
					m_TempString.append(removeNoiseSemicolon(strValue.trim()) + subTemp.m_strNewFieldSplitSign);
				}
			} else {
				if (strValue.trim().equals("")) {
					// m_TempString.append("0");
					m_TempString.append(nvl);
				} else {
					// change on 2010-04-06 将;替换为空格
					// m_TempString.append(strValue.trim());
					m_TempString.append(removeNoiseSemicolon(strValue.trim()));

				}
			}
		}

		return m_TempString.toString();
	}

	private String removeNoiseSemicolon(String content) {
		// 字段中不能出现；
		String strValue = content.replaceAll(";", " ");
		return strValue;
	}

	@Override
	public String toString() {
		String strTaskID = "Line-Parser-" + (collectObjInfo == null ? "NULL" : collectObjInfo.getTaskID());
		return strTaskID;
	}
}
