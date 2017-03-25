package parser;

import java.io.FileReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import task.CollectObjInfo;
import templet.SectTempletP;
import templet.SectTempletP.FieldTemplet;
import framework.ConstDef;

public class SectParser extends Parser {

	// 剩于未解析的字符串
	private String m_OddString = new String();

	private String CommonKeyField = "";

	public SectParser() {
	}

	public SectParser(CollectObjInfo collectInfo) {
		super(collectInfo);

		SectTempletP templet = (SectTempletP) (collectObjInfo.getParseTemplet());
		templet.m_strHeadSectSplitSign = ConstDef.ParseFilePath(templet.m_strHeadSectSplitSign, collectInfo.getLastCollectTime());
		templet.m_strTailSectSplitSign = ConstDef.ParseFilePath(templet.m_strTailSectSplitSign, collectInfo.getLastCollectTime());
	}

	public boolean parseData() throws Exception {
		FileReader reader = null;

		try {
			reader = new FileReader(fileName);
			char[] buff = new char[65536];
			StringBuffer sb = new StringBuffer();

			int iLen = 0;
			while ((iLen = reader.read(buff)) > 0) {
				sb.append(new String(buff, 0, iLen));
			}

			// 加结束标记
			sb.append("\n**FILEEND**\n");
			BuildData(sb.toString().toCharArray(), sb.length());
		} finally {
			try {
				if (reader != null)
					reader.close();
			} catch (Exception e) {
			}
		}

		return true;
	}

	public boolean BuildData(char[] byData, int nbyLength) {
		// 结合前次未解析完的数据
		m_OddString += new String(byData, 0, nbyLength);
		SectTempletP templet = (SectTempletP) (collectObjInfo.getParseTemplet());

		switch (templet.m_nSectScanType) {
			case ConstDef.COLLECT_SECT_SCANTYPE_N : {
				// \n\n来分
				String[] m_SectData = m_OddString.split("\n\n");
				// 最后一段不解析,留给下一行解析
				if (m_SectData.length > 1)
					m_OddString = m_SectData[m_SectData.length - 1];
				for (int i = 0; i < m_SectData.length; i++) {
					// 开始分段解析
					ParseSectData(m_SectData[i].toString().trim());
				}
			}
				break;
			case ConstDef.COLLECT_SECT_SCANTYPE_KEYWORD : {
				int nFirstIndex = m_OddString.indexOf(templet.m_strHeadSectSplitSign);

				if (templet.m_strHeadSectSplitSign.equals("$S"))
					nFirstIndex = 0;
				int nLastIndex = m_OddString.indexOf(templet.m_strTailSectSplitSign);

				if (templet.m_strTailSectSplitSign.equals("$E"))
					nLastIndex = m_OddString.length() - 1;

				SectTempletP.SectTemplet secttemp = templet.m_SectTemplet.get(0);

				int nTempIndex = m_OddString.indexOf(secttemp.m_strCommonFieldList);
				if (secttemp.m_strCommonFieldList.trim() != null && !"".equals(secttemp.m_strCommonFieldList.trim()) && nTempIndex >= 0
						&& nTempIndex < nLastIndex) {
					CommonKeyField = m_OddString.substring(nTempIndex + secttemp.m_strCommonFieldList.length(), nTempIndex
							+ secttemp.m_strCommonFieldList.length() + 4);
				}
				// 利用前后关键字来分段
				String strSectInfo = "";
				while (nFirstIndex >= 0 && nLastIndex >= 0) {
					strSectInfo = "";
					// if(templet.m_strHeadSectSplitSign.equals("$S"))
					// nFirstIndex = 0;
					// else
					// nFirstIndex = nFirstIndex +
					// templet.m_strHeadSectSplitSign.length();

					strSectInfo = m_OddString.substring(nFirstIndex, nLastIndex);

					// 开始逐段分析,开始的是按照段分析的不能Split
					ParseSectData(strSectInfo.trim());

					if (nLastIndex + templet.m_strTailSectSplitSign.length() > m_OddString.length()) {
						m_OddString = "";
						break;
					}
					m_OddString = m_OddString.substring(nLastIndex + templet.m_strTailSectSplitSign.length());

					nFirstIndex = m_OddString.indexOf(templet.m_strHeadSectSplitSign);
					if (templet.m_strHeadSectSplitSign.equals("$S"))
						nFirstIndex = 0;
					nLastIndex = m_OddString.indexOf(templet.m_strTailSectSplitSign);
					if (templet.m_strTailSectSplitSign.equals("$E"))
						nLastIndex = m_OddString.length() - 1;

					nTempIndex = m_OddString.indexOf(secttemp.m_strCommonFieldList);
					if (secttemp.m_strCommonFieldList.trim() != null && !"".equals(secttemp.m_strCommonFieldList.trim()) && nTempIndex >= 0
							&& nTempIndex < nLastIndex) {
						CommonKeyField = m_OddString.substring(nTempIndex + secttemp.m_strCommonFieldList.length(), nTempIndex
								+ secttemp.m_strCommonFieldList.length() + 4);
					}
				}
			}
				break;
			default :
				break;
		}
		return false;
	}

	// 根据关键字来判断是属于什么段类型
	private SectTempletP.SectTemplet GetSectTempletType(String strSectInfo) {
		SectTempletP templet = (SectTempletP) (collectObjInfo.getParseTemplet());
		SectTempletP.SectTemplet sectTemp = null;
		boolean isExist = false;
		for (int i = 0; i < templet.m_SectTemplet.size(); i++) {
			sectTemp = templet.m_SectTemplet.get(i);
			switch (sectTemp.m_nSectKeySearchType) {
			// 根据开始的关键字来判断属于那一个段
				case ConstDef.COLLECT_SECT_KEYWORD_HEAD :
					if (strSectInfo.indexOf(sectTemp.m_strSectKeyWord) == 0)
						isExist = true;
					break;
				// 根据结束的关键字来判断属于那一个段
				case ConstDef.COLLECT_SECT_KEYWORD_TAIL :
					if (strSectInfo.lastIndexOf(sectTemp.m_strSectKeyWord) == (strSectInfo.length() - sectTemp.m_strSectKeyWord.length()))
						isExist = true;
					break;
				// 根据中间的关键字来判断属于那一个段
				case ConstDef.COLLECT_SECT_KEYWORD_SENTER :

					String[] strKeyWord = sectTemp.m_strSectKeyWord.split(";");

					isExist = true;
					// 需要同时满足几个条件
					for (int k = 0; k < strKeyWord.length; k++) {
						if (strSectInfo.indexOf(strKeyWord[k]) < 0) {
							isExist = false;
							break;
						}
					}
					break;
			}
			if (isExist)// 表示已经查找到是属于什么段
				break;
			else
				sectTemp = null;
		}
		return sectTemp;
	}

	private void ParseSectData(String strSectInfo) {
		try {
			// sectTemp为某个段的模板
			SectTempletP.SectTemplet sectTemp = GetSectTempletType(strSectInfo);
			if (sectTemp == null) {
				// 不能确定的段类型
				log.error(this.collectObjInfo.getTaskID() + ": can't find the templet for the section . " + strSectInfo);
				return;
			}
			StringBuffer strNewBuffer = new StringBuffer();
			// 每行添加OMCID
			strNewBuffer.append(collectObjInfo.getDevInfo().getOmcID());
			strNewBuffer.append(sectTemp.m_strNewSplitSign);
			// 添加当前时间 格式YYYY-MM-DD HH24:MI:SS 并添加新的分隔符号
			Date now = new Date();
			SimpleDateFormat spformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			String strTime = spformat.format(now);
			strNewBuffer.append(strTime + sectTemp.m_strNewSplitSign);

			strTime = spformat.format(collectObjInfo.getLastCollectTime());
			// StampTime
			strNewBuffer.append(strTime + sectTemp.m_strNewSplitSign);

			int nCurrentPos = 0;// 当前的位置

			if (CommonKeyField != null && CommonKeyField.trim() != "")
				strNewBuffer.append(CommonKeyField.trim() + sectTemp.m_strNewSplitSign);

			boolean subfiledflag = false;
			int i = 0;
			for (i = 0; i < sectTemp.m_FieldTemplet.size(); i++) {
				SectTempletP.FieldTemplet field = sectTemp.m_FieldTemplet.get(i);
				switch (field.m_nParseType) {
					case ConstDef.COLLECT_SECT_PARSE_BITPOS :// 根据开始位置与长度来解析
						subfiledflag = false;
						String strValue1 = strSectInfo.substring(field.m_nStartPos, field.m_nStartPos + field.m_nDataLength);
						// System.out.println(strValue1);
						nCurrentPos = field.m_nStartPos + field.m_nDataLength;
						strNewBuffer.append(strValue1.trim() + sectTemp.m_strNewSplitSign);
						// System.out.println(nCurrentPos);
						break;
					case ConstDef.COLLECT_SECT_PARSE_KEYWORD :// 根据前后关键字来解析
					{
						// System.out.println(field.m_strFieldName);
						subfiledflag = false;
						int nStartIndex = nCurrentPos;
						int nEndIndex = nStartIndex + field.m_strHeadFieldSign.length();
						if (field.m_strHeadFieldSign.equals("$S"))
							nStartIndex = 0;
						else {
							if (strSectInfo.indexOf(field.m_strHeadFieldSign, nStartIndex) >= 0)
								nStartIndex = strSectInfo.indexOf(field.m_strHeadFieldSign, nStartIndex) + field.m_strHeadFieldSign.length();
							else {
								strNewBuffer.append("" + sectTemp.m_strNewSplitSign);
								continue;
							}
						}
						if (field.m_strHeadFieldSign.equals("$E"))
							nEndIndex = strSectInfo.length() - 1;
						else
							nEndIndex = strSectInfo.indexOf(field.m_strTailFieldSign, nStartIndex + 1);

						// System.out.println(nStartIndex);
						// System.out.println(nEndIndex);
						if (nStartIndex < 0) {
							// 不能找到的数据就设置为0
							strNewBuffer.append("" + sectTemp.m_strNewSplitSign);
						}
						String strValue2 = "";
						if (nEndIndex < 0)
							strValue2 = strSectInfo.substring(nStartIndex);
						else
							strValue2 = strSectInfo.substring(nStartIndex, nEndIndex);
						strValue2 = strValue2.trim();
						if (strValue2.indexOf((char) 10) >= 0)
							strValue2 = strValue2.replace((char) 10, (char) 124);

						nCurrentPos = nEndIndex;
						strNewBuffer.append(strValue2 + sectTemp.m_strNewSplitSign);
					}
						break;
					case ConstDef.COLLECT_SECT_PARSE_TOEND : {
						subfiledflag = false;
						int nStartIndex = nCurrentPos;
						nStartIndex = strSectInfo.indexOf(field.m_strHeadFieldSign, nStartIndex) + field.m_strHeadFieldSign.length();
						String strValue3 = strSectInfo.substring(nStartIndex);
						strNewBuffer.append(strValue3.trim() + sectTemp.m_strNewSplitSign);
					}
						break;
					case ConstDef.COLLECT_SECT_PARSE_KEYFIELDONLYONE :// 根据前后关键字来解析
					{
						// System.out.println(field.m_strFieldName);
						subfiledflag = false;
						int nStartIndex = 0;
						int nEndIndex = 0 + field.m_strHeadFieldSign.length();
						if (field.m_strHeadFieldSign.equals("$S"))
							nStartIndex = 0;
						else {
							if (strSectInfo.indexOf(field.m_strHeadFieldSign, nStartIndex) >= 0)
								nStartIndex = strSectInfo.indexOf(field.m_strHeadFieldSign, nStartIndex) + field.m_strHeadFieldSign.length();
							else {
								strNewBuffer.append("" + sectTemp.m_strNewSplitSign);
								continue;
							}
						}
						if (field.m_strHeadFieldSign.equals("$E"))
							nEndIndex = strSectInfo.length() - 1;
						else
							nEndIndex = strSectInfo.indexOf(field.m_strTailFieldSign, nStartIndex + 1);

						if (nStartIndex < 0) {
							// 不能找到的数据就设置为0
							strNewBuffer.append("" + sectTemp.m_strNewSplitSign);
						}
						String strValue2 = "";
						if (nEndIndex < 0)
							strValue2 = strSectInfo.substring(nStartIndex);
						else
							strValue2 = strSectInfo.substring(nStartIndex, nEndIndex);

						strValue2 = strValue2.trim();
						if (strValue2.indexOf((char) 10) >= 0)
							strValue2 = strValue2.replace((char) 10, (char) 124);

						nCurrentPos = nEndIndex;
						strNewBuffer.append(strValue2 + sectTemp.m_strNewSplitSign);
					}
						break;
					case ConstDef.COLLECT_SECT_PARSE_COMPLEX :// 根据子字段来解析
					{
						// 设置每列在表头中的索引
						String columnLine = sectTemp.headLine;
						// 此种方式默认表头的分隔符与数据的分隔符一致
						if (columnLine != null && !columnLine.equals("")) {
							String[] columns = columnLine.split(sectTemp.m_FieldTemplet.get(i).m_strSubFieldColSplitSign);
							// //
							// DistributeTemplet.TableTemplet distTableTmp =
							// collectObjInfo.getDistributeTemplet().tableTemplets.get(sectTemp.m_nSectTypeIndex);
							// Map<Integer,
							// distributor.DistributeTemplet.FieldTemplet> map1
							// = distTableTmp.fields;
							// for (int j = 0; j < columns.length; j++)
							// {
							// for (int j2 = 0; j2 < map1.size(); j2++)
							// {
							// DistributeTemplet.FieldTemplet column =
							// map1.get(j2);
							// // 如果分发模板中存在此列，那么就记下该列的索引
							// if ( column.rawName.equals(columns[j]) )
							// {
							// column.dataIndex = j;
							// break;
							// }
							// }
							// }
							// //
							Map<Integer, FieldTemplet> map = sectTemp.m_FieldTemplet.get(i).m_SubFieldTemplet;
							for (int j = 0; j < columns.length; j++) {
								for (int j2 = 0; j2 < map.size(); j2++) {
									SectTempletP.FieldTemplet column = map.get(j2);
									if (column.m_strFieldName.equals(columns[j])) {
										column.dataIndex = j;
										break;
									}
								}
							}
						}
						//
						subfiledflag = true;
						// System.out.println(strSectInfo);
						int nStartIndex = strSectInfo.indexOf(field.m_strHeadFieldSign);
						nStartIndex += field.m_strHeadFieldSign.length();

						int nEndIndex = strSectInfo.indexOf(field.m_strTailFieldSign, nStartIndex + 1);

						if (nEndIndex < 0 && field.m_strTailFieldSign.equals("$E"))
							nEndIndex = strSectInfo.length();
						// 根据前后的标记,得到全部行型数据
						String strTemp = strSectInfo.substring(nStartIndex, nEndIndex);
						// 分行
						String[] strRows = strTemp.split(field.m_strSubFieldRowSplitSign);
						// 逐行分析
						StringBuffer TempBuild = new StringBuffer();
						for (int k = 0; k < strRows.length; k++) {
							String strRow = strRows[k].trim();
							if (strRow.trim().equals(""))
								continue;
							String strValue = ParseFieldData(strRow, sectTemp, sectTemp.m_FieldTemplet.get(i));

							if (strValue == null || strValue.equals(""))
								continue;
							TempBuild.delete(0, TempBuild.length());
							TempBuild.append(strNewBuffer.toString() + strValue + "\n");

							distribute.DistributeData(TempBuild.toString().getBytes(), sectTemp.m_nSectTypeIndex);
						}
					}
						break;

				}

			}

			if (!subfiledflag) {
				// TODO 存在的理由是啥？ strNewBuffer.append(0);
				strNewBuffer.append("\n");
				distribute.DistributeData(strNewBuffer.toString().getBytes(), sectTemp.m_nSectTypeIndex);
			}
		} catch (Exception e) {
			log.error("解析段出错:", e);
		}
	}

	/*
	 * 解析子段信息 strSubSectInfo 原始子段内容, ResultInfo 解析后的内容, sectTemp 段信息 sectField 节信息 返回是否存在数据,当不存在数据时返回 false
	 */
	private String ParseFieldData(String strSubSectInfo, SectTempletP.SectTemplet sectTemp, SectTempletP.FieldTemplet sectField) {
		StringBuffer strNewBuffer = new StringBuffer();
		Map<Integer, FieldTemplet> subFieldTemplets = sectField.m_SubFieldTemplet;
		// //
		// DistributeTemplet.TableTemplet distTableTmp =
		// collectObjInfo.getDistributeTemplet().tableTemplets.get(sectTemp.m_nSectTypeIndex);
		// Map<Integer, distributor.DistributeTemplet.FieldTemplet> map =
		// distTableTmp.fields;
		// //
		try {
			// 是否按行截取
			if (sectField.m_bSubSectSplit) {
				String[] strColData;
				if (sectField.m_strSubFieldColSplitSign.equals("\\\\|"))
					strColData = strSubSectInfo.split("\\|");
				else
					strColData = strSubSectInfo.split(sectField.m_strSubFieldColSplitSign);
				boolean isNullData = true;// 判断是否是空行
				// 遍历当前行的所有列
				if (sectTemp.headLine != null && !sectTemp.headLine.equals("")) {
					// //
					// for (int i = 0; i < strColData.length; i++)
					// {
					// // m_SubFieldTemplet，所有的子列，就是记录每列数据索引的列
					// // 如果数据strColData[i]所在的列i在子列中存在，那么就列数据就需要，否则不需要
					// for (int j = 0; j < map.size(); j++)
					// {
					// DistributeTemplet.FieldTemplet field = map.get(j);
					// if ( field.dataIndex == i )
					// {
					// if ( strColData[i].trim().equals("") )
					// strNewBuffer.append("0"
					// + sectTemp.m_strNewSplitSign);
					// else
					// {
					// isNullData = false;
					// strNewBuffer.append(strColData[i].trim()
					// + sectTemp.m_strNewSplitSign);
					// }
					// break;
					// }
					// }
					// }
					// //
					for (int i = 0; i < strColData.length; i++) {
						// m_SubFieldTemplet，所有的子列，就是记录每列数据索引的列
						// 如果数据strColData[i]所在的列i在子列中存在，那么就列数据就需要，否则不需要
						for (int j = 0; j < subFieldTemplets.size(); j++) {
							FieldTemplet field = subFieldTemplets.get(j);
							if (field.dataIndex == i) {
								if (strColData[i].trim().equals(""))
									strNewBuffer.append("0" + sectTemp.m_strNewSplitSign);
								else {
									isNullData = false;
									strNewBuffer.append(strColData[i].trim() + sectTemp.m_strNewSplitSign);
								}
								break;
							}
						}
					}
				} else {
					for (int i = 0; i < strColData.length; i++) {
						if (sectField.m_SubFieldTemplet.containsKey(i)) {
							if (strColData[i].trim().equals(""))
								strNewBuffer.append("0" + sectTemp.m_strNewSplitSign);
							else {
								isNullData = false;
								strNewBuffer.append(strColData[i].trim() + sectTemp.m_strNewSplitSign);
							}
						}
					}
				}
				if (isNullData)// 当截取后所有字段内容都为空,认为该行是空行,可以不保留
					return "";
			} else {
				int nCurrentPos = 0;// 当前的位置
				// 根据模板类型来截取
				for (int i = 0; i < sectField.m_SubFieldTemplet.size(); i++) {
					SectTempletP.FieldTemplet field = sectField.m_SubFieldTemplet.get(i);
					switch (field.m_nParseType) {
						case ConstDef.COLLECT_SECT_PARSE_BITPOS :// 根据开始位置与长度来解析
							String strValue1 = strSubSectInfo.substring(field.m_nStartPos, field.m_nStartPos + field.m_nDataLength);

							nCurrentPos = field.m_nStartPos + field.m_nDataLength;
							strNewBuffer.append(strValue1 + sectTemp.m_strNewSplitSign);
							// System.out.println(strValue1);
							// System.out.println(nCurrentPos);
							break;
						case ConstDef.COLLECT_SECT_PARSE_KEYWORD :// 根据前后关键字来解析
						{
							int nStartIndex = nCurrentPos;
							int nEndIndex = nStartIndex + field.m_strHeadFieldSign.length();
							nStartIndex = strSubSectInfo.indexOf(field.m_strHeadFieldSign, nStartIndex) + field.m_strHeadFieldSign.length();
							nEndIndex = strSubSectInfo.indexOf(field.m_strTailFieldSign, nStartIndex + 1);
							// System.out.println(nStartIndex);
							// System.out.println(nEndIndex);
							if (nStartIndex < 0) {
								// 不能找到的数据就设置为0
								strNewBuffer.append("0" + sectTemp.m_strNewSplitSign);
							}
							String strValue2 = "";
							if (nEndIndex < 0)
								strValue2 = strSubSectInfo.substring(nStartIndex);
							else
								strValue2 = strSubSectInfo.substring(nStartIndex, nEndIndex);
							nCurrentPos = nEndIndex;
							strNewBuffer.append(strValue2 + sectTemp.m_strNewSplitSign);
						}
							break;
						case ConstDef.COLLECT_SECT_PARSE_TOEND : {
							int nStartIndex = nCurrentPos;
							nStartIndex = strSubSectInfo.indexOf(field.m_strHeadFieldSign, nStartIndex) + field.m_strHeadFieldSign.length();
							String strValue3 = strSubSectInfo.substring(nStartIndex);
							strNewBuffer.append(strValue3 + sectTemp.m_strNewSplitSign);
						}
							break;
						case ConstDef.COLLECT_SECT_PARSE_COMPLEX :// 根据子字段来解析
						{
							// 子字段中嵌套子字段的数据
							strNewBuffer.append(ParseFieldData(strSubSectInfo, sectTemp, sectField));
						}
							break;
					}// switch
				}// for
			}// else
		}// try
		catch (Exception Err) {
			Err.printStackTrace();
		}
		return strNewBuffer.toString();
	}
}
