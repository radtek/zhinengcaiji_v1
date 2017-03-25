package parser.eric.cm;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import util.CommonDB;
import util.LogMgr;
import util.Util;

/**
 * 联通一期参数解析。
 * 
 * @author 陈思江 2010-3-19
 */
public class EricssonV1CmParserImp implements EricssonCmParser {

	private String bscName;

	private Logger logger = LogMgr.getInstance().getSystemLogger();

	private Map<String, String> flags = new HashMap<String, String>();

	private GsmEriCmSQLLDR sl;

	private final List<String> TABLES = new ArrayList<String>(); // 保存所有的表名（已经存在或者已经建立的）

	private static final String TABLE_PREFIX = "CLT_CM_ERIR12_"; // 表名的前缀

	private static final String FLAG_RLLBP = "<RLLBP;";

	private static final String FLAG_RAEPP = "<RAEPP:ID=ALL;";

	private static final String FLAG_RLDEP = "<RLDEP:CELL=ALL;";

	private static final String FLAG_RLCPP = "<RLCPP:CELL=ALL;";

	private static final String FLAG_RLCHP = "<RLCHP:CELL=ALL;";

	private static final String FLAG_RLCRP = "<RLCRP:CELL=ALL;";

	private static final String FLAG_RLCFP = "<RLCFP:CELL=ALL;";

	private static final String FLAG_RLGSP = "<RLGSP:CELL=ALL;";

	private static final String FLAG_RLMFP = "<RLMFP:CELL=ALL;";

	private static final String FLAG_RLPCP = "<RLPCP:CELL=ALL;";

	private static final String FLAG_RLSBP = "<RLSBP:CELL=ALL;";

	private static final String FLAG_RLIMP = "<RLIMP:CELL=ALL;";

	private static final String FLAG_RLSSP = "<RLSSP:CELL=ALL;";

	private static final String FLAG_RLLOP = "<RLLOP:CELL=ALL;";

	private static final String FLAG_RLLPP = "<RLLPP:CELL=ALL;";

	private static final String FLAG_RLLFP = "<RLLFP:CELL=ALL;";

	private static final String FLAG_RLLDP = "<RLLDP:CELL=ALL;";

	private static final String FLAG_RLLHP = "<RLLHP:CELL=ALL;";

	private static final String FLAG_RLIHP = "<RLIHP:CELL=ALL;";

	private static final String FLAG_RLBCP = "<RLBCP:CELL=ALL;";

	private static final String FLAG_RLLCP = "<RLLCP:CELL=ALL;";

	private static final String FLAG_RLNRP = "<RLNRP:CELL=ALL;";

	private static final String FLAG_RLDEP_EXT = "<RLDEP:CELL=ALL,EXT;";

	private static final String FLAG_RXMOP = "<RXMOP:MOTY=RXOTRX;";

	private static final String FLAG_RLCXP = "<RLCXP:CELL=ALL;";

	private static final String FLAG_RLHPP = "<RLHPP:CELL=ALL;";

	private static final String FLAG_RLDEP_3G = "<RLDEP:CELL=ALL,UTRAN,EXT;";

	private static final String FLAG_RLNRP_3G = "<RLNRP:CELL=ALL,UTRAN;";

	private static final String FLAG_RLDHP = "<RLDHP:CELL=ALL;";

	private static final String FLAG_RLLUP = "<RLLUP:CELL=ALL;";

	private static final String FLAG_RXAPP = "<RXAPP:MOTY=RXOTG;";

	private static final String FLAG_RXMOP_TG = "<RXMOP:MOTY=RXOTG;";

	private static final String FLAG_RLSTP = "<RLSTP:CELL=ALL;";

	private static final String FLAG_RLBDP = "<RLBDP:CELL=ALL;";

	private static final String FLAG_IOEXP = "<IOEXP;";

	private static final String FLAG_RXTCP = "<RXTCP:MOTY=RXOTG;";

	private static final String FLAG_RXMOP_TX = "<RXMOP:MOTY=RXOTX;";

	private static final String FLAG_RXMOP_RX = "<RXMOP:MOTY=RXORX;";

	private static final String FLAG_RXMOP_CF = "<RXMOP:MOTY=RXOCF;";

	private static final String FLAG_RXMOP_RLCLP = "<RLCLP:CELL=ALL;";

	@Override
	public void parse(String file, int omcId, Timestamp stampTime, long taskID) throws Exception {

		File f = new File(file);
		bscName = f.getName().replaceAll(".log", "");
		sl = new GsmEriCmSQLLDR(taskID, stampTime, omcId, bscName);
		FileInputStream in = null;

		BufferedReader reader = null;

		String currentType = null;
		String currentTableName = null;
		List<String> datas = new ArrayList<String>();
		try {
			in = new FileInputStream(f);
			reader = new BufferedReader(new InputStreamReader(in));
			sl.readyForSqlldr();
			String line = null;
			while ((line = reader.readLine()) != null) {
				if (Util.isNull(line)) {
					continue;
				} else if (line.startsWith("CELL") && line.endsWith("AND BCCH DATA")) {
					continue;
				} else if (line.startsWith("CELL") && line.endsWith("DATA")) {
					continue;
				} else if (line.startsWith("CELL") && line.endsWith("RESOURCES")) {
					continue;
				} else if (line.startsWith("BSC") && line.endsWith("DATA")) {
					continue;
				} else if (line.startsWith("CELL") && line.endsWith("FREQUENCIES")) {
					continue;
				} else if (line.startsWith("DYNAMIC") && line.endsWith("DATA")) {
					continue;
				} else if (line.startsWith("NEIGHBOUR") && line.endsWith("DATA")) {
					continue;
				} else if (line.startsWith("RADIO") && line.endsWith("ADMINISTRATION")) {
					continue;
				} else if (line.startsWith("MANAGED") && line.endsWith("DATA")) {
					continue;
				} else if (line.startsWith("CONNECTION") && line.endsWith("DATA")) {
					continue;
				} else if (line.equals("ABIS PATH STATUS")) {
					continue;
				} else if (line.startsWith("TG TO") && line.endsWith("CONNECTION DATA")) {
					continue;
				} else if (line.trim().startsWith("*")) {
					continue;
				} else if (line.startsWith("RTTI ") && line.endsWith(" IAN")) {
					continue;
				} else if (line.trim().equals("TEST MEASUREMENT FREQUENCIES")) {
					continue;
				}
				if (line.equals("END") || line.endsWith("FUNCTION BUSY")) {
					if (datas.size() != 0) {
						handleData(datas, currentType, currentTableName);
					}
					continue;
				}

				if (isStart(line) != null) {
					currentType = isStart(line);
					currentTableName = getTableName(currentType);
					continue;
				}

				datas.add(line);
			}
			sl.sqlldr();

		} catch (Exception e) {
			throw new Exception("解析爱立信（联通一期）参数文件时异常，文件：" + file, e);
		} finally {

			if (in != null) {
				try {
					in.close();
				} catch (IOException e) {
				}
			}
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
				}
			}
		}
	}

	/**
	 * 是否是一个类型数据的开始。如果是，返回该该类型的标致，比如 "<RLLBP;"，如果不是，返回null.
	 * 
	 * @param line
	 * @return
	 */
	private String isStart(String line) {
		if (flags.size() == 0) {
			Field[] fields = getClass().getDeclaredFields();
			for (Field f : fields) {
				int modif = f.getModifiers();
				if (Modifier.isStatic(modif) && Modifier.isFinal(modif) && f.getName().startsWith("FLAG_")) {
					try {
						flags.put(f.getName(), f.get(f).toString());
					} catch (Exception e) {
						logger.error(e.getMessage(), e);
					}
				}
			}
		}

		Iterator<Map.Entry<String, String>> pairs = flags.entrySet().iterator();
		while (pairs.hasNext()) {
			String val = pairs.next().getValue();
			if (line.startsWith(val)) {
				return val;
			}
		}

		return null;
	}

	/**
	 * 根据<RLCFP:CELL=ALL;这样的标记，获得要入库的表名。
	 * 
	 * @param type
	 * @return
	 */
	private String getTableName(String type) {
		if (type.equalsIgnoreCase(FLAG_RLNRP_3G)) {
			return TABLE_PREFIX + "RLNRP_3G";
		}
		if (type.equalsIgnoreCase(FLAG_RLDEP_3G)) {
			return TABLE_PREFIX + "RLDEP_3G";
		}
		if (type.equalsIgnoreCase(FLAG_RXMOP_TG)) {
			return TABLE_PREFIX + "RXMOP_TG";
		}
		if (type.equals(FLAG_RXMOP_TX)) {
			return TABLE_PREFIX + "RXMOP_TX";
		}
		if (type.equals(FLAG_RXMOP_RX)) {
			return TABLE_PREFIX + "RXMOP_RX";
		}
		if (type.equals(FLAG_RXMOP_CF)) {
			return TABLE_PREFIX + "RXMOP_CF";
		}
		String tableName = type.replace("<", "");
		tableName = tableName.replace(":", "_");
		tableName = tableName.replace(";", "");
		tableName = tableName.replace("CELL=ALL", "");
		tableName = tableName.replace("ID=ALL", "");
		tableName = tableName.replace(",", "");
		tableName = tableName.replace("MOTY=RXOTRX", "");
		tableName = tableName.replace("MOTY=RXOTG", "");
		if (tableName.endsWith("_")) {
			tableName = tableName.substring(0, tableName.length() - 1);
		}

		return TABLE_PREFIX + tableName;
	}

	/**
	 * 分割出数据列头
	 * 
	 * @param line
	 * @return 每列的列名，及列的开始结束位置
	 */
	private List<ColInfo> splitColHeader(String line) {
		List<ColInfo> row = new ArrayList<ColInfo>();

		char[] chars = line.trim().toCharArray();

		int start = -1;
		List<Character> tmp = new ArrayList<Character>();
		for (int index = 0; index < chars.length; index++) {
			char c = chars[index];

			if (c != 0x20) {
				if (start == -1) {
					start = index;
				}
				tmp.add(c);
			}
			if (c == 0x20 || index == chars.length - 1) {
				char[] tmparray = new char[tmp.size()];
				for (int i = 0; i < tmparray.length; i++) {
					tmparray[i] = tmp.get(i);
				}
				ColInfo ci = new ColInfo(new String(tmparray), start, index == chars.length - 1 ? chars.length : index);
				if (Util.isNotNull(ci.colText)) {
					row.add(ci);
				}
				start = -1;
				tmp.clear();
			}
		}

		return row;
	}

	/**
	 * 分割出一行数据。根据列头信息（开始及结束位置）来分割的。
	 * 
	 * @param line
	 * @param row
	 * @param isSpec
	 * @return 每一列对应的数据，如果没有，返回null
	 */
	private List<String> splitValues(String line, List<ColInfo> row, boolean isSpec) {
		List<String> values = new ArrayList<String>();
		String tmpLine = line
				+ "                                                                                                                                                                     ";
		for (ColInfo colInfo : row) {
			String val = tmpLine.substring(colInfo.startIndex, colInfo.endIndex);
			if (Util.isNull(val)) {
				values.add("null");
			} else {
				String v = subStringFromLine(tmpLine, colInfo.startIndex, colInfo.endIndex, isSpec).trim();
				values.add(v.replaceAll("'", "''"));
			}
		}
		return values;
	}

	/**
	 * 从一行原始文本中，取出指定位置的数据。
	 * 
	 * @param line
	 * @param startIndex
	 * @param endIndex
	 * @param spec
	 * @return
	 */
	private String subStringFromLine(String line, int startIndex, int endIndex, boolean spec) {
		String s = line.substring(startIndex);

		int newEnd = endIndex - startIndex;

		char[] chars = s.toCharArray();
		boolean findWord = false;
		for (int i = 0; i < chars.length; i++) {
			char c = chars[i];
			if (c != 0x20) {
				findWord = true;
			}
			if (spec) {
				if (i > newEnd && c == 0x20 && findWord) {
					return s.substring(0, i);
				}
			} else {
				if (c == 0x20 && findWord) {
					return s.substring(0, i);
				}
			}
		}

		return "null";
	}

	/**
	 * 处理各个类型的数据
	 * 
	 * @param datas
	 * @param type
	 * @param tableName
	 * @return
	 */
	private List<String> handleData(List<String> datas, String type, String tableName) {
		List<String> inserts = null;

		if (type.equalsIgnoreCase(FLAG_RLLBP) || type.equalsIgnoreCase(FLAG_RLDEP) || type.equalsIgnoreCase(FLAG_RLSBP)
				|| type.equalsIgnoreCase(FLAG_RLDEP_EXT)) {
			inserts = handleType1(datas, tableName);
		} else if (type.equalsIgnoreCase(FLAG_RAEPP)) {
			inserts = handleType2(datas, tableName);
		} else if (type.equalsIgnoreCase(FLAG_RLCPP) || type.equalsIgnoreCase(FLAG_RLCRP)) {
			inserts = handleType3(datas, tableName);
		} else if (type.equalsIgnoreCase(FLAG_RLCHP)) {
			inserts = handleType4(datas, tableName);
		} else if (type.equalsIgnoreCase(FLAG_RLCFP)) {
			inserts = handleType18(datas, tableName);
		} else if (type.equalsIgnoreCase(FLAG_RLGSP) || type.equalsIgnoreCase(FLAG_RLLFP)) {
			inserts = handleType5(datas, tableName);
		} else if (type.equalsIgnoreCase(FLAG_RLLHP)) {
			inserts = handleType17(datas, tableName);
		} else if (type.equalsIgnoreCase(FLAG_RLMFP)) {
			inserts = handleType6(datas, tableName);
		} else if (type.equalsIgnoreCase(FLAG_RLPCP)) {
			inserts = handleType7(datas, tableName);
		} else if (type.equalsIgnoreCase(FLAG_RLIMP) || type.equalsIgnoreCase(FLAG_RLLPP) || type.equalsIgnoreCase(FLAG_RLLDP)
				|| type.equalsIgnoreCase(FLAG_RLLCP) || type.equalsIgnoreCase(FLAG_RLCXP) || type.equalsIgnoreCase(FLAG_RLHPP)
				|| type.equalsIgnoreCase(FLAG_RLLUP))

		{
			inserts = handleType8(datas, tableName);
		} else if (type.equalsIgnoreCase(FLAG_RLSSP)) {
			inserts = handleType9(datas, tableName);
		} else if (type.equalsIgnoreCase(FLAG_RLLOP)) {
			inserts = handleType10(datas, tableName);
		} else if (type.equalsIgnoreCase(FLAG_RLIHP)) {
			inserts = handleType11(datas, tableName);
		} else if (type.equalsIgnoreCase(FLAG_RLBCP)) {
			inserts = handleType12(datas, tableName);
		} else if (type.equalsIgnoreCase(FLAG_RLNRP)) {
			inserts = handleType13(datas, tableName);
		} else if (type.equalsIgnoreCase(FLAG_RXMOP)) {
			inserts = handleType14(datas, tableName);
		} else if (type.equalsIgnoreCase(FLAG_RLNRP_3G)) {
			inserts = handleType15(datas, tableName);
		} else if (type.equalsIgnoreCase(FLAG_RLDEP_3G)) {
			inserts = handleType16(datas, tableName);
		} else if (type.equalsIgnoreCase(FLAG_RLDHP)) {
			inserts = handleType19(datas, tableName);
		} else if (type.equalsIgnoreCase(FLAG_RXAPP)) {
			inserts = handleType20(datas, tableName);
		} else if (type.equalsIgnoreCase(FLAG_RXMOP_TG)) {
			inserts = handleType21(datas, tableName);
		} else if (type.equalsIgnoreCase(FLAG_RLSTP)) {
			inserts = handleType22(datas, tableName);
		} else if (type.equalsIgnoreCase(FLAG_RLBDP)) {
			inserts = handleType23(datas, tableName);
		} else if (type.equalsIgnoreCase(FLAG_IOEXP)) {
			inserts = handleType24(datas, tableName);

		} else if (type.equalsIgnoreCase(FLAG_RXTCP)) {
			inserts = handleType25(datas, tableName);
		} else if (type.equalsIgnoreCase(FLAG_RXMOP_TX)) {
			inserts = handleType26(datas, tableName);
		} else if (type.equalsIgnoreCase(FLAG_RXMOP_RX)) {
			inserts = handleType27(datas, tableName);
		} else if (type.equalsIgnoreCase(FLAG_RXMOP_CF)) {
			inserts = handleType28(datas, tableName);
		} else if (type.equalsIgnoreCase(FLAG_RXMOP_RLCLP)) {
			inserts = handleType29(datas, tableName);

		}

		datas.clear();

		return inserts;
	}

	/**
	 * 处理的节点： <RLLBP; <RLDEP:CELL=ALL; <RLSBP:CELL=ALL; <RLDEP:CELL=ALL,EXT;
	 * 
	 * @param datas
	 * @param tableName
	 * @return
	 */
	private List<String> handleType1(List<String> datas, String tableName) {
		List<String> inserts = new ArrayList<String>();

		List<ColInfo> cols = new ArrayList<ColInfo>();
		List<String> values = new ArrayList<String>();
		for (int i = 0; i < datas.size(); i++) {
			String line = datas.get(i);
			int rowNum = i + 1;
			if (rowNum % 2 != 0) {
				cols.addAll(splitColHeader(line));
			} else {
				values.addAll(splitValues(line, splitColHeader(datas.get(i - 1)), false));
			}
			if (rowNum % 6 == 0) {
				inserts.add(createInsert(tableName, cols, values));
				cols.clear();
				values.clear();
			}
		}

		return inserts;
	}

	/**
	 * 处理的节点： <RAEPP:ID=ALL;
	 * 
	 * @param datas
	 * @param tableName
	 * @return
	 */
	private List<String> handleType2(List<String> datas, String tableName) {
		List<String> inserts = new ArrayList<String>();
		List<ColInfo> cols = new ArrayList<ColInfo>();
		List<String> values = new ArrayList<String>();
		for (int i = 0; i < datas.size(); i++) {
			String line = datas.get(i);
			String content = line.trim().split(" ")[0];
			String[] items = content.trim().split("-");
			if (items.length == 2) {
				ColInfo c = new ColInfo(items[0], 0, 0);
				String v = items[1];
				if (!cols.contains(c)) {
					cols.add(c);
					values.add(v);
				}
			}
		}
		if (cols.size() != 0 && values.size() != 0) {
			inserts.add(createInsert(tableName, cols, values));
		}

		return inserts;
	}

	/**
	 * 处理的节点： <RLCPP:CELL=ALL; <RLCRP:CELL=ALL;
	 * 
	 * @param datas
	 * @param tableName
	 * @return
	 */
	private List<String> handleType3(List<String> datas, String tableName) {
		List<String> inserts = new ArrayList<String>();
		List<ColInfo> cols = null;
		List<String> values = null;
		for (int i = 0; i < datas.size(); i++) {
			String line = datas.get(i);
			if (i == 0) {
				cols = splitColHeader(line);
			} else {
				if (tableName.equalsIgnoreCase("CLT_CM_ERIR12_RLCRP")) {
					values = splitValues(line, cols, true);
				} else {
					values = splitValues(line, cols, false);
				}
				inserts.add(createInsert(tableName, cols, values));
			}
		}
		return inserts;
	}

	/**
	 * 处理的节点： <RLCHP:CELL=ALL; <RLCFP:CELL=ALL;
	 * 
	 * @param datas
	 * @param tableName
	 * @return
	 */
	private List<String> handleType4(List<String> datas, String tableName) {
		List<String> inserts = new ArrayList<String>();
		List<ColInfo> cols = null;
		List<String> values = null;

		String cell = null;

		for (int i = 0; i < datas.size(); i++) {
			String line = datas.get(i);

			if (line.trim().equalsIgnoreCase("CELL")) {
				cols = null;
				values = null;
				cell = datas.get(i + 1).trim();
				i++;
			} else if (cell != null) {
				if (cols == null) {
					cols = splitColHeader(line);
				} else {
					ColInfo cellCol = new ColInfo("CELL", 0, 0);
					if (cols.contains(cellCol)) {
						cols.remove(0);
					}
					values = splitValues(line, cols, false);
					if (!cols.contains(cellCol)) {
						cols.add(0, cellCol);
					}

					values.add(0, cell);
					inserts.add(createInsert(tableName, cols, values));
				}
			}
		}

		return inserts;
	}

	/**
	 * 处理节点： <RLGSP:CELL=ALL; <RLLFP:CELL=ALL;
	 * 
	 * @param datas
	 * @param tableName
	 * @return
	 */
	private List<String> handleType5(List<String> datas, String tableName) {
		List<String> inserts = new ArrayList<String>();
		boolean isNotNext = false;
		List<ColInfo> cols = new ArrayList<ColInfo>();
		List<String> values = new ArrayList<String>();
		for (int i = 0; i < datas.size(); i++) {
			String line = datas.get(i);
			int rowNum = i + 1;
			if (rowNum % 2 != 0) {
				cols.addAll(splitColHeader(line));

				if (i + 1 < datas.size() && datas.get(i + 1).trim().startsWith("CELL")) {
					rowNum++;
					values.add("null");
					values.add("null");
					isNotNext = true;
					continue;
				}
			} else if (rowNum % 2 == 0 && !isNotNext) {
				values.addAll(splitValues(line, splitColHeader(datas.get(i - 1)), false));
			}
			if (rowNum % 4 == 0) {
				isNotNext = false;
				inserts.add(createInsert(tableName, cols, values));
				cols.clear();
				values.clear();
			}
		}

		return inserts;
	}

	/**
	 * 处理节点 ： <RLMFP:CELL=ALL;
	 * 
	 * @param datas
	 * @param tableName
	 * @return
	 */
	private List<String> handleType6(List<String> datas, String tableName) {
		List<String> inserts = new ArrayList<String>();

		List<ColInfo> cols = new ArrayList<ColInfo>();
		List<String> values = new ArrayList<String>();

		String cell = null;
		String lis = null;

		for (int i = 0; i < datas.size(); i++) {
			String line = datas.get(i).trim();

			if (line.equalsIgnoreCase("CELL") && cell == null) {
				cell = datas.get(i + 1).trim();
				i++;
			} else if (line.equalsIgnoreCase("LISTTYPE") && cell != null) {
				lis = datas.get(i + 1).trim();
				i++;
			} else if (line.equalsIgnoreCase("MBCCHNO") && cell != null && lis != null) {
				if ((i + 1) == datas.size() || datas.get(i + 1).equals("CELL")) {
					cols.clear();
					values.clear();
					continue;
				} else {
					String nline = datas.get(i + 1);
					String nnline = null;
					if (i + 2 < datas.size()) {
						nnline = datas.get(i + 2);
						if (nnline.trim().equals("LISTTYPE")) {
							nnline = null;
						}
					}
					if (nnline != null) {
						nline = nline.trim() + " " + nnline.trim();
					}
					String[] sp = nline.split(" ", 99);
					StringBuilder sb = new StringBuilder();
					for (String x : sp) {
						if (Util.isNotNull(x))
							sb.append(x.trim()).append(",");
					}
					sb.delete(sb.length() - 1, sb.length());
					nline = sb.toString();
					cols.add(new ColInfo("CELL", 0, 0));
					cols.add(new ColInfo("LISTTYPE", 0, 0));
					cols.add(new ColInfo("MBCCHNO", 0, 0));
					values.add(cell);
					values.add(lis);
					values.add(nline);
					inserts.add(createInsert(tableName, cols, values));
					cell = null;
					lis = null;
					cols.clear();
					values.clear();
				}
			}
		}

		return inserts;
	}

	/**
	 * 处理节点：<RLPCP:CELL=ALL;
	 * 
	 * @param datas
	 * @param tableName
	 * @return
	 */
	private List<String> handleType7(List<String> datas, String tableName) {
		List<String> inserts = new ArrayList<String>();

		List<ColInfo> cols = new ArrayList<ColInfo>();
		List<String> values = new ArrayList<String>();
		for (int i = 0; i < datas.size(); i++) {
			String line = datas.get(i);
			int rowNum = i + 1;
			if (rowNum % 2 != 0) {
				cols.addAll(splitColHeader(line));
			} else {
				values.addAll(splitValues(line, splitColHeader(datas.get(i - 1)), false));
			}
			if (rowNum % 4 == 0) {
				if (cols.size() == 7 && cols.get(0).colText.equals("CELL")) {
					inserts.add(createInsert(tableName, cols, values));
				}
				cols.clear();
				values.clear();
			}
		}

		return inserts;
	}

	/**
	 * 新版本<RLLUP:CELL=ALL;。
	 * 
	 * @param datas
	 * @param tableName
	 * @return
	 */
	private List<String> handleType30(List<String> datas, String tableName) {
		List<String> inserts = new ArrayList<String>();

		List<ColInfo> cols = null;
		List<String> values = null;

		for (int i = 0; i < datas.size(); i++) {
			String line = datas.get(i);
			if (line.startsWith("CELL ")) {
				cols = splitColHeader(line);
				String nLine = datas.get(i + 1);
				i += 1;
				values = splitValues(nLine, cols, false);
			} else if (line.startsWith("SCTYPE")) {
				List<ColInfo> tmp = splitColHeader(line);
				cols.addAll(tmp);
				String nLine = datas.get(i + 1);
				i += 1;
				values.addAll(splitValues(nLine, tmp, false));
				inserts.add(createInsert(tableName, cols, values));
				cols.clear();
				values.clear();
			}
		}
		return inserts;
	}

	/**
	 * 处理节点： <RLIMP:CELL=ALL; <RLLPP:CELL=ALL; <RLLDP:CELL=ALL; <RLLCP:CELL=ALL; <RLHPP:CELL=ALL; <RLCXP:CELL=ALL;
	 * 
	 * @param datas
	 * @param tableName
	 * @return
	 */
	private List<String> handleType8(List<String> datas, String tableName) {
		if (tableName.equals("CLT_CM_ERIR12_RLLUP")) {
			if (datas.get(0).startsWith("CELL") && datas.get(4).startsWith("CELL")) {
				// 处理新版本。
				return handleType30(datas, tableName);
			}
		}
		List<String> inserts = new ArrayList<String>();

		List<ColInfo> cols = null;
		List<String> values = null;

		for (int i = 0; i < datas.size(); i++) {
			String line = datas.get(i);
			if (i == 0) {
				cols = splitColHeader(line);
			} else {
				values = splitValues(line, cols, false);
				inserts.add(createInsert(tableName, cols, values));
				values.clear();
				values = null;
			}
		}

		return inserts;
	}

	/**
	 * 处理节点： <RLSSP:CELL=ALL;
	 * 
	 * @param datas
	 * @param tableName
	 * @return
	 */
	private List<String> handleType9(List<String> datas, String tableName) {
		List<String> inserts = new ArrayList<String>();

		List<ColInfo> cols = new ArrayList<ColInfo>();
		List<String> values = new ArrayList<String>();
		for (int i = 0; i < datas.size(); i++) {
			String line = datas.get(i).trim();
			if (line.startsWith("RLINKTAFR ") && line.endsWith(" RLINKTAWB")) {
				line = "RLINKTAFR  RLINKTAHR";
			}

			if (line.startsWith("CELL")) {
				List<ColInfo> ci = splitColHeader(line);
				cols.addAll(ci);
				values.addAll(splitValues(datas.get(i + 1), ci, false));
				i++;
			} else if (line.startsWith("ACCMIN")) {
				List<ColInfo> ci = splitColHeader(line);
				cols.addAll(ci);
				values.addAll(splitValues(datas.get(i + 1), ci, false));
				i++;
			} else if (line.startsWith("NCCPERM")) {
				cols.add(new ColInfo("NCCPERM", 0, 0));
				String nline = datas.get(i + 1);
				if (Util.isNull(nline)) {
					nline = "";
				} else {
					String[] sp = nline.split(" ", 99);
					StringBuilder sb = new StringBuilder();
					for (String x : sp) {
						if (Util.isNotNull(x))
							sb.append(x.trim()).append(",");
					}
					sb.delete(sb.length() - 1, sb.length());
					nline = sb.toString();
					sb = null;
				}
				values.add(nline);
				i++;
			} else if (line.startsWith("RLINKTAFR")) {
				List<ColInfo> ci = splitColHeader(line);
				cols.addAll(ci);
				if ((i + 1) == datas.size() || datas.get(i + 1).trim().equals("CELL")) {
					values.add("");
					values.add("");
					inserts.add(createInsert(tableName, cols, values));
					cols.clear();
					values.clear();
					continue;
				} else
					values.addAll(splitValues(datas.get(i + 1), ci, false));
				i++;

				inserts.add(createInsert(tableName, cols, values));
				cols.clear();
				values.clear();
			}
		}

		return inserts;
	}

	/**
	 * 处理节点：<RLLOP:CELL=ALL;
	 * 
	 * @param datas
	 * @param tableName
	 * @return
	 */
	private List<String> handleType10(List<String> datas, String tableName) {
		List<String> inserts = new ArrayList<String>();

		List<ColInfo> cols = new ArrayList<ColInfo>();
		List<String> values = new ArrayList<String>();
		int rowNum = 0;
		for (int i = 0; i < datas.size(); i++) {
			rowNum++;
			String line = datas.get(i);
			String nextLine = datas.size() > i + 1 ? datas.get(i + 1) : null;

			if (line.trim().startsWith("BCCHREUSE")) {
				rowNum++;
				cols.addAll(splitColHeader(line));
				if (nextLine != null && !nextLine.trim().startsWith("BCCHDTCBHYST")) {
					values.addAll(splitValues(nextLine, splitColHeader(line), false));
				} else {
					values.add("null");
					values.add("null");
					values.add("null");
					values.add("null");
					values.add("null");
				}
				continue;
			}

			if (line.trim().startsWith("BCCHDTCBHYST")) {
				rowNum++;
				cols.addAll(splitColHeader(line));
				if (nextLine != null && !nextLine.trim().startsWith("SCTYPE")) {
					values.addAll(splitValues(nextLine, splitColHeader(line), false));
				} else {
					values.add("null");
				}
				continue;
			}

			if (rowNum % 2 != 0) {
				cols.addAll(splitColHeader(line));
			} else {
				values.addAll(splitValues(line, splitColHeader(datas.get(i - 1)), false));
			}

			if (rowNum % 8 == 0) {
				if (cols.size() == 23 && cols.get(0).colText.equals("CELL")) {
					inserts.add(createInsert(tableName, cols, values));
				}
				cols.clear();
				values.clear();
			}
		}
		return inserts;
	}

	/**
	 * 处理节点：<RLIHP:CELL=ALL;
	 * 
	 * @param datas
	 * @param tableName
	 * @return
	 */
	private List<String> handleType11(List<String> datas, String tableName) {
		List<String> inserts = new ArrayList<String>();

		List<ColInfo> cols = new ArrayList<ColInfo>();
		List<String> values = new ArrayList<String>();
		for (int i = 0; i < datas.size(); i++) {
			String line = datas.get(i);

			if (line.startsWith("SSOFFSETULAWBP") && line.endsWith("SSOFFSETDLAWBN")) {
				if (!datas.get(i + 1).startsWith("QOFFSETULAWBP")) {
					i += 1;
				}
				continue;
			}
			if (line.startsWith("QOFFSETULAWBP") && line.endsWith("QOFFSETDLAWBN")) {
				if ((i + 1) < datas.size() && !datas.get(i + 1).startsWith("CELL")) {
					i += 1;
				}
				continue;
			}

			int rowNum = i + 1;
			if (rowNum % 2 != 0) {
				cols.addAll(splitColHeader(line));
			} else {
				values.addAll(splitValues(line, splitColHeader(datas.get(i - 1)), false));
			}
			if (rowNum % 8 == 0) {
				inserts.add(createInsert(tableName, cols, values));
				cols.clear();
				values.clear();
			}
		}

		return inserts;
	}

	/**
	 * 处理的节点： <RLBCP:CELL=ALL;
	 * 
	 * @param datas
	 * @param tableName
	 * @return
	 */
	private List<String> handleType12(List<String> datas, String tableName) {
		List<String> inserts = new ArrayList<String>();

		List<ColInfo> cols = new ArrayList<ColInfo>();
		List<String> values = new ArrayList<String>();
		for (int i = 0; i < datas.size(); i++) {
			String line = datas.get(i);
			int rowNum = i + 1;
			if (rowNum % 2 != 0) {
				cols.addAll(splitColHeader(line));
			} else {
				values.addAll(splitValues(line, splitColHeader(datas.get(i - 1)), false));
			}
			if (rowNum % 6 == 0) {
				if (cols.size() == 9 && cols.get(0).colText.equals("CELL")) {
					inserts.add(createInsert(tableName, cols, values));
				}
				cols.clear();
				values.clear();
			}
		}

		return inserts;
	}

	/**
	 * 处理节点：<RLNRP:CELL=ALL;
	 * 
	 * @param datas
	 * @param tableName
	 * @return
	 */
	private List<String> handleType13(List<String> datas, String tableName) {
		List<String> inserts = new ArrayList<String>();

		List<ColInfo> cols = new ArrayList<ColInfo>();
		List<String> values = new ArrayList<String>();

		String cell = null;

		for (int i = 0; i < datas.size(); i++) {
			String line = datas.get(i).trim();
			if (line.equalsIgnoreCase("CELL")) {
				cell = datas.get(i + 1).trim();
				i++;
			} else if (line.startsWith("CELLR")) {
				String nline = datas.get(i + 1);
				if (nline.equals("NONE")) {
					cols.add(new ColInfo("CELL", 0, 0));
					values.add(cell);
					inserts.add(createInsert(tableName, cols, values));
					cols.clear();
					values.clear();
				} else {
					List<ColInfo> ci = splitColHeader(line);
					cols.addAll(ci);
					values.addAll(splitValues(nline, ci, false));
					i++;
				}
			} else if (line.startsWith("KHYST")) {
				List<ColInfo> ci = splitColHeader(line);
				cols.addAll(ci);
				values.addAll(splitValues(datas.get(i + 1), ci, false));
				i++;
			} else if (line.startsWith("TRHYST")) {
				List<ColInfo> ci = splitColHeader(line);
				cols.addAll(ci);
				values.addAll(splitValues(datas.get(i + 1), ci, false));
				i++;
			} else if (line.startsWith("HIHYST")) {
				List<ColInfo> ci = splitColHeader(line);
				cols.addAll(ci);
				values.addAll(splitValues(datas.get(i + 1), ci, false));
				i++;

				cols.add(0, new ColInfo("CELL", 0, 0));
				values.add(0, cell);
				inserts.add(createInsert(tableName, cols, values));
				cols.clear();
				values.clear();
			}
		}

		return inserts;
	}

	/**
	 * 处理节点：<RXMOP:MOTY=RXOTRX;
	 * 
	 * @param datas
	 * @param tableName
	 * @return
	 */
	@SuppressWarnings("unused")
	private List<String> handleType14xx(List<String> datas, String tableName) {
		List<String> inserts = new ArrayList<String>();

		List<ColInfo> cols = new ArrayList<ColInfo>();
		List<String> values = new ArrayList<String>();
		int rowNum = 0;
		for (int i = 0; i < datas.size(); i++) {
			rowNum++;
			String line = datas.get(i).trim();
			if (rowNum % 2 != 0) {
				cols.addAll(splitColHeader(line));
			} else {
				values.addAll(splitValues(line, splitColHeader(datas.get(i - 1)), false));
			}
			if (rowNum % 4 == 0) {
				String nextLine = datas.size() > i + 1 ? datas.get(i + 1) : null;
				if (nextLine != null && !nextLine.trim().equals("MO") && nextLine.trim().length() < 5) {
					// rowNum--;
					i++;
					int index = values.size() - 2;
					if (index > 0) {
						String v = values.get(index);
						int n = Integer.parseInt(nextLine.trim());
						v = (n - 1 + "," + n);
						values.remove(index);
						values.add(index, v);
					}
				}
				inserts.add(createInsert(tableName, cols, values));
				cols.clear();
				values.clear();
			}
		}

		return inserts;
	}

	/**
	 * 处理节点：<RXMOP:MOTY=RXOTRX;
	 * 
	 * @param datas
	 * @param tableName
	 * @return
	 */
	private List<String> handleType14(List<String> datas, String tableName) {
		List<String> inserts = new ArrayList<String>();

		List<ColInfo> cols = new ArrayList<ColInfo>();
		List<String> values = new ArrayList<String>();
		for (int i = 0; i < datas.size(); i++) {
			String line = datas.get(i).trim();
			if (line.startsWith("MO")) {
				List<ColInfo> tmp = splitColHeader(line);
				cols.addAll(tmp);
				values.addAll(splitValues(datas.get(i + 1).trim(), tmp, false));
				i += 1;
			} else if (line.startsWith("SWVERREPL")) {
				List<ColInfo> tmp = splitColHeader(line);
				cols.addAll(tmp);
				values.addAll(splitValues(datas.get(i + 1).trim(), tmp, false));
				i += 1;
				for (int j = i + 1; j < datas.size(); j++) {
					if (datas.get(j).startsWith("MO")) {
						break;
					}
					String rawDcp2 = values.get(values.size() - 1);
					rawDcp2 = rawDcp2 + "," + datas.get(j).trim();
					values.set(values.size() - 1, rawDcp2);
					i += 1;
				}
				inserts.add(createInsert(tableName, cols, values));
				cols.clear();
				values.clear();
			} else {
				continue;
			}

		}

		return inserts;
	}

	/**
	 * 处理节点：<RLNRP:CELL=ALL,UTRAN;
	 * 
	 * @param datas
	 * @param tableName
	 * @return
	 */
	private List<String> handleType15(List<String> datas, String tableName) {
		List<String> inserts = new ArrayList<String>();

		String cell = "";

		for (int i = 0; i < datas.size(); i++) {
			String line = datas.get(i).trim();
			if (line.equalsIgnoreCase("CELL")) {
				cell = datas.get(i + 1);
				i += 1;
			} else if (line.toUpperCase().startsWith("CELLR")) {
				List<ColInfo> cols = splitColHeader(line);
				List<String> values = splitValues(datas.get(i + 1).trim(), cols, false);
				i += 1;
				cols.add(new ColInfo("CELL", -1, -1));
				values.add(cell);
				inserts.add(createInsert(tableName, cols, values));
			}

		}

		return inserts;
	}

	/**
	 * 处理节点：<RLDEP:CELL=ALL,UTRAN,EXT;
	 * 
	 * @param datas
	 * @param tableName
	 * @return
	 */
	private List<String> handleType16(List<String> datas, String tableName) {
		List<String> inserts = new ArrayList<String>();
		List<ColInfo> cols = null;
		List<String> values = null;
		for (int i = 0; i < datas.size(); i++) {
			String line = datas.get(i);
			if (line.startsWith("CELLIND")) {
				cols.add(new ColInfo("CELLIND", -1, -1));
				values.add(datas.get(i + 1).replace("'", "''"));
				i += 1;
				inserts.add(createInsert(tableName, cols, values));
				cols = null;
				values = null;
			} else if (line.startsWith("CELL")) {
				cols = splitColHeader(line);
				values = splitValues(datas.get(i + 1), cols, false);
				i += 1;
			}
		}
		return inserts;
	}

	/**
	 * 处理节点：<RLLHP:CELL=ALL;
	 * 
	 * @param datas
	 * @param tableName
	 * @return
	 */
	private List<String> handleType17(List<String> datas, String tableName) {
		List<String> inserts = new ArrayList<String>();

		List<ColInfo> cols = new ArrayList<ColInfo>();
		List<String> values = new ArrayList<String>();
		for (int i = 0; i < datas.size(); i++) {
			String line = datas.get(i).trim();
			if (line.startsWith("CELL")) {
				List<ColInfo> tmp = splitColHeader(line);
				cols.addAll(tmp);
				values.addAll(splitValues(datas.get(i + 1).trim(), tmp, false));
				i += 1;
			} else if (line.startsWith("HCSIN")) {
				List<ColInfo> tmp = splitColHeader(line);
				cols.addAll(tmp);
				if (i + 1 < datas.size()) {
					String next = datas.get(i + 1).trim();
					if (next.startsWith("CELL")) {
						values.add("null");
						values.add("null");
						inserts.add(createInsert(tableName, cols, values));
						cols.clear();
						values.clear();
						continue;
					} else {
						values.addAll(splitValues(next, tmp, false));
						i += 1;
					}
					inserts.add(createInsert(tableName, cols, values));
					cols.clear();
					values.clear();
				}
			} else {
				continue;
			}

		}

		return inserts;
	}

	/**
	 * 处理节点：<RLCFP:CELL=ALL;
	 * 
	 * @param datas
	 * @param tableName
	 * @return
	 */
	private List<String> handleType18(List<String> datas, String tableName) {
		List<String> inserts = new ArrayList<String>();
		List<ColInfo> cols = new ArrayList<ColInfo>();
		String cell = "";
		for (int i = 0; i < datas.size(); i++) {
			String line = datas.get(i);
			if (line.startsWith("CELL")) {
				cell = datas.get(i + 1);
				i += 1;
			} else if (line.startsWith("CHGR")) {
				cols.clear();
				cols.addAll(splitColHeader(line));
			} else {
				List<List<String>> okList = new ArrayList<List<String>>();
				String nextLine = line;
				List<String> vals = new ArrayList<String>();
				do {
					if (vals.size() == 0) {
						vals.addAll(splitValues(nextLine, cols, false));
					} else {
						List<String> tmpVals = splitValues(nextLine, cols, false);
						String tChgr = tmpVals.get(0);
						if (tChgr.equals("null")) {
							for (int j = 0; j < tmpVals.size(); j++) {
								String tmpV = tmpVals.get(j);
								String vV = vals.get(j);
								if (!tmpV.equals("null") && !vV.equals("null")) {
									vals.set(j, vV + "," + tmpV);
								} else if (!tmpV.equals("null")) {
									vals.set(j, tmpV);
								}
							}
						} else {
							List<String> cpList = new ArrayList<String>();
							cpList.addAll(vals);
							okList.add(cpList);
							vals.clear();
							vals.addAll(splitValues(nextLine, cols, false));
						}
					}
					if (((i + 1) < datas.size())) {
						nextLine = datas.get(i + 1);
						if (!nextLine.trim().equals("CELL"))
							i += 1;
						else
							nextLine = null;
					} else {
						nextLine = null;
					}
				} while (nextLine != null);

				cols.add(new ColInfo("CELL", 0, 0));
				for (List<String> oneList : okList) {
					oneList.add(cell);
					inserts.add(createInsert(tableName, cols, oneList));
				}

				if (vals.size() > 0) {
					vals.add(cell);
					inserts.add(createInsert(tableName, cols, vals));
				}
				cols.clear();
			}
		}

		return inserts;

		// List<String> inserts = new ArrayList<String>();
		//
		// List<ColInfo> cols = new ArrayList<ColInfo>();
		// List<String> values = new ArrayList<String>();
		// for (int i = 0; i < datas.size(); i++)
		// {
		// String line = datas.get(i);
		// if ( line.startsWith("CELL") )
		// {
		// if ( cols.size() > 0 )
		// {
		// inserts.add(createInsert(tableName, cols, values));
		// cols.clear();
		// values.clear();
		// }
		// List<ColInfo> tmp = splitColHeader(line);
		// cols.addAll(tmp);
		// values.addAll(splitValues(datas.get(i + 1), tmp, false));
		// i += 1;
		// }
		// else if ( line.startsWith("CHGR") )
		// {
		// List<ColInfo> tmp = splitColHeader(line);
		// cols.addAll(tmp);
		// values.addAll(splitValues(datas.get(i + 1), tmp, false));
		// i += 1;
		// }
		// else
		// {
		// line = line.trim();
		// String[] splits = line.split(" ");
		// if ( splits != null )
		// {
		// if ( splits.length > 1 )
		// {
		// String tn = splits[0];
		// String dchno = splits[splits.length - 1];
		// String s = values.get(5);
		// s += ("," + tn);
		// values.set(5, s);
		// s = values.get(9);
		// s += ("," + dchno);
		// values.set(9, s);
		// }
		// else
		// {
		// String dchno = splits[0];
		// String s = null;
		// if ( values.size() > 9 )
		// s = values.get(9);
		// if ( values.size() > 9 )
		// s += ("," + dchno);
		// else
		// s = dchno;
		// if ( values.size() > 9 )
		// values.set(9, s);
		// }
		// }
		// }
		// }
		// inserts.add(createInsert(tableName, cols, values));
		// cols.clear();
		// values.clear();
		//
		// return inserts;
	}

	/**
	 * 处理节点： <RLDHP:CELL=ALL;
	 * 
	 * @param datas
	 * @param tableName
	 * @return
	 */
	private List<String> handleType19(List<String> datas, String tableName) {
		List<String> inserts = new ArrayList<String>();

		List<ColInfo> cols = new ArrayList<ColInfo>();
		List<String> values = new ArrayList<String>();
		for (int i = 0; i < datas.size(); i++) {
			String line = datas.get(i);
			if (line.startsWith("CELL ") && i + 1 < datas.size()) {
				List<ColInfo> tmp = splitColHeader(line);
				cols.addAll(tmp);
				values.addAll(splitValues(datas.get(i + 1), tmp, false));
				inserts.add(createInsert(tableName, cols, values));
				cols.clear();
				values.clear();
				i += 1;
			}
		}
		return inserts;
	}

	/**
	 * 处理节点：<RXAPP:MOTY=RXOTG;
	 * 
	 * @param datas
	 * @param tableName
	 * @return
	 */
	private List<String> handleType20(List<String> datas, String tableName) {
		List<String> inserts = new ArrayList<String>();

		String mo = "";
		String dev = "";
		String dcp = "";
		List<ColInfo> cols = null;
		for (int i = 0; i < datas.size(); i++) {
			String line = datas.get(i);
			if (line.equals("MO")) {
				mo = datas.get(i + 1);
				i += 1;
				cols = null;
			} else if (line.startsWith("DEV") && line.endsWith("TEI")) {
				cols = splitColHeader(line);
			} else if (cols != null) {
				List<String> values = splitValues(line, cols, false);
				List<ColInfo> tmpCol = new ArrayList<EricssonV1CmParserImp.ColInfo>();
				if (values.get(0).equals("null"))
					values.set(0, dev);
				else
					dev = values.get(0);
				if (values.get(1).equals("null"))
					values.set(1, dcp);
				else
					dcp = values.get(1);
				tmpCol.addAll(cols);
				tmpCol.add(new ColInfo("MO", 0, 0));
				values.add(mo);
				ColInfo k = findColInfoInList(tmpCol, "64K");
				if (k != null)
					k.colText = "COL_64K";
				inserts.add(createInsert(tableName, tmpCol, values));
			}
		}

		return inserts;
	}

	/**
	 * 处理节点：<RXMOP:MOTY=RXOTG;
	 * 
	 * @param datas
	 * @param tableName
	 * @return
	 */
	private List<String> handleType21(List<String> datas, String tableName) {
		List<String> inserts = new ArrayList<String>();

		List<ColInfo> cols = new ArrayList<EricssonV1CmParserImp.ColInfo>();
		List<String> vals = new ArrayList<String>();
		for (int i = 0; i < datas.size(); i++) {
			String line = datas.get(i);
			String trimLine = line.trim();
			if (trimLine.startsWith("MO") && trimLine.endsWith("MODEL")) {
				cols.clear();
				vals.clear();
				List<ColInfo> tmp = splitColHeader(line);
				cols.addAll(tmp);
				vals.addAll(splitValues(datas.get(i + 1), tmp, false));
				i += 1;
			} else if (trimLine.startsWith("SWVERREPL") && trimLine.endsWith("TMODE")) {
				String tmpLine1 = line.substring(line.indexOf("SWVERREPL"));
				String tmpLine2 = datas.get(i + 1).substring(line.indexOf("SWVERREPL"));
				List<ColInfo> tmp = splitColHeader(tmpLine1);
				cols.addAll(tmp);
				vals.addAll(splitValues(tmpLine2, tmp, false));
				i += 1;
			} else if (trimLine.startsWith("CONFMD") && trimLine.endsWith("SCGR")) {
				String tmpLine1 = line.substring(line.indexOf("CONFMD"));
				String tmpLine2 = datas.get(i + 1).substring(line.indexOf("CONFMD"));
				List<ColInfo> tmp = splitColHeader(tmpLine1);
				cols.addAll(tmp);
				vals.addAll(splitValues(tmpLine2, tmp, false));
				i += 1;
			} else if (trimLine.startsWith("ABIS64KTHR") && trimLine.endsWith("DFRMAABISTHR")) {
				String tmpLine1 = line.substring(line.indexOf("ABIS64KTHR"));
				String nextLine = datas.get(i + 1);
				if (nextLine.trim().startsWith("DAMRCR")) {
					cols.addAll(splitColHeader(tmpLine1));
					vals.add("");
					vals.add("");
					vals.add("");
				} else {
					String tmpLine2 = nextLine.substring(line.indexOf("ABIS64KTHR"));
					List<ColInfo> tmp = splitColHeader(tmpLine1);
					cols.addAll(tmp);
					vals.addAll(splitValues(tmpLine2, tmp, false));
					i += 1;
				}
			} else if (trimLine.startsWith("DAMRCR") && trimLine.endsWith("DAMRREDABISTHR")) {
				String tmpLine1 = line.substring(line.indexOf("DAMRCR"));
				String nextLine = datas.get(i + 1);
				if (nextLine.trim().startsWith("SDHRAABISTHR")) {
					cols.addAll(splitColHeader(tmpLine1));
					vals.add("");
					vals.add("");
				} else {
					String tmpLine2 = nextLine.substring(line.indexOf("DAMRCR"));
					List<ColInfo> tmp = splitColHeader(tmpLine1);
					cols.addAll(tmp);
					vals.addAll(splitValues(tmpLine2, tmp, false));
					i += 1;
				}
			} else if (trimLine.startsWith("SDHRAABISTHR") && trimLine.endsWith("SDAMRREDABISTHR")) {
				String tmpLine1 = line.substring(line.indexOf("SDHRAABISTHR"));
				String nextLine = datas.get(i + 1);
				if (nextLine.trim().startsWith("PTA")) {
					cols.addAll(splitColHeader(tmpLine1));
					vals.add("");
					vals.add("");
					vals.add("");
				} else {
					String tmpLine2 = nextLine.substring(line.indexOf("SDHRAABISTHR"));
					List<ColInfo> tmp = splitColHeader(tmpLine1);
					cols.addAll(tmp);
					vals.addAll(splitValues(tmpLine2, tmp, false));
					i += 1;
				}
			} else if (trimLine.startsWith("PTA") && trimLine.endsWith("JBSDL")) {
				String tmpLine1 = line.substring(line.indexOf("PTA"));
				String nextLine = datas.get(i + 1);
				if (nextLine.trim().startsWith("TGFID")) {
					cols.addAll(splitColHeader(tmpLine1));
					vals.add("");
					vals.add("");
				} else {
					String tmpLine2 = nextLine.substring(line.indexOf("PTA"));
					List<ColInfo> tmp = splitColHeader(tmpLine1);
					cols.addAll(tmp);
					vals.addAll(splitValues(tmpLine2, tmp, false));
					i += 1;
				}
			} else if (trimLine.startsWith("TGFID") && trimLine.endsWith("PACKALG")) {
				String tmpLine1 = line.substring(line.indexOf("TGFID"));
				String nextLine = datas.get(i + 1);
				String tmpLine2 = nextLine.substring(line.indexOf("TGFID"));
				List<ColInfo> tmp = splitColHeader(tmpLine1);
				cols.addAll(tmp);
				vals.addAll(splitValues(tmpLine2, tmp, false));
				i += 1;
				inserts.add(createInsert(tableName, cols, vals));
				cols.clear();
				vals.clear();
			}
		}

		return inserts;
	}

	/**
	 * 处理节点：<RLSTP:CELL=ALL;
	 * 
	 * @param datas
	 * @param tableName
	 * @return
	 */
	private List<String> handleType22(List<String> datas, String tableName) {
		List<String> inserts = new ArrayList<String>();
		List<ColInfo> cols = new ArrayList<EricssonV1CmParserImp.ColInfo>();
		cols.add(new ColInfo("CELL", 0, 0));
		cols.add(new ColInfo("STATE", 0, 0));
		boolean find = false;
		for (int i = 0; i < datas.size(); i++) {
			String line = datas.get(i).trim();
			if (find) {
				if (line.startsWith("CELL"))
					continue;

				List<String> vals = new ArrayList<String>();
				String[] sp = line.split(" ");
				vals.add(sp[0].trim());
				vals.add(sp[sp.length - 1].trim());
				inserts.add(createInsert(tableName, cols, vals));
			} else if (line.startsWith("CELL") && line.endsWith("STATE")) {
				find = true;
			}

		}
		return inserts;
	}

	/**
	 * 处理节点：<RLBDP:CELL=ALL;
	 * 
	 * @param datas
	 * @param tableName
	 * @return
	 */
	private List<String> handleType23(List<String> datas, String tableName) {
		List<String> inserts = new ArrayList<String>();
		List<ColInfo> cols = new ArrayList<EricssonV1CmParserImp.ColInfo>();
		List<String> vals = new ArrayList<String>();
		for (int i = 0; i < datas.size(); i++) {
			String line = datas.get(i);
			if (line.equalsIgnoreCase("CELL")) {
				cols.clear();
				vals.clear();
				cols.add(new ColInfo("CELL", 0, 0));
				vals.add(datas.get(i + 1).trim());
				i += 1;
			} else if (line.startsWith("CHGR") && line.endsWith("EACPREF")) {
				List<ColInfo> tmp = splitColHeader(line);
				cols.addAll(tmp);
				vals.addAll(splitValues(datas.get(i + 1), tmp, false));
				i += 1;
				inserts.add(createInsert(tableName, cols, vals));
				cols.clear();
				vals.clear();
			}
		}
		return inserts;
	}

	/**
	 * 处理节点：<IOEXP;
	 * 
	 * @param datas
	 * @param tableName
	 * @return
	 */
	private List<String> handleType24(List<String> datas, String tableName) {
		List<String> inserts = new ArrayList<String>();
		for (int i = 0; i < datas.size(); i++) {
			String line = datas.get(i);
			if (line.trim().equals("IDENTITY")) {
				List<ColInfo> cols = new ArrayList<EricssonV1CmParserImp.ColInfo>();
				cols.add(new ColInfo("IDENTITY", 0, 0));
				List<String> vals = new ArrayList<String>();
				vals.add(datas.get(i + 1).trim());
				inserts.add(createInsert(tableName, cols, vals));
				return inserts;
			}
		}
		return inserts;
	}

	/**
	 * 处理节点：<RXTCP:MOTY=RXOTG;
	 * 
	 * @param datas
	 * @param tableName
	 * @return
	 */
	private List<String> handleType25(List<String> datas, String tableName) {
		List<String> inserts = new ArrayList<String>();
		List<ColInfo> cols = null;
		String mo = "";
		String cell = "";
		for (int i = 0; i < datas.size(); i++) {
			String line = datas.get(i);
			if (cols == null && line.startsWith("MO") && line.endsWith("CHGR")) {
				cols = splitColHeader(line);
			} else if (cols != null) {
				List<String> vals = splitValues(line, cols, false);
				if (!vals.get(0).equals("null"))
					mo = vals.get(0);
				else
					vals.set(0, mo);

				if (!vals.get(1).equals("null"))
					cell = vals.get(1);
				else
					vals.set(1, cell);
				inserts.add(createInsert(tableName, cols, vals));
			}
		}
		return inserts;
	}

	/**
	 * 处理节点：<RXMOP:MOTY=RXOTX;
	 * 
	 * @param datas
	 * @param tableName
	 * @return
	 */
	private List<String> handleType26(List<String> datas, String tableName) {
		List<String> inserts = new ArrayList<String>();
		List<ColInfo> cols = null;
		for (int i = 0; i < datas.size(); i++) {
			String line = datas.get(i);
			if (cols == null && line.startsWith("MO") && line.endsWith("MPWR")) {
				cols = splitColHeader(line);
			} else if (cols != null && !line.startsWith("MO")) {
				inserts.add(createInsert(tableName, cols, splitValues(line, cols, false)));
			}
		}
		return inserts;
	}

	/**
	 * 处理节点：<RXMOP:MOTY=RXORX;
	 * 
	 * @param datas
	 * @param tableName
	 * @return
	 */
	private List<String> handleType27(List<String> datas, String tableName) {
		List<String> inserts = new ArrayList<String>();
		List<ColInfo> cols = null;
		for (int i = 0; i < datas.size(); i++) {
			String line = datas.get(i);
			if (cols == null && line.startsWith("MO") && line.endsWith("ANTB")) {
				cols = splitColHeader(line);
			} else if (cols != null && !line.startsWith("MO")) {
				inserts.add(createInsert(tableName, cols, splitValues(line, cols, false)));
			}
		}
		return inserts;
	}

	/**
	 * 处理节点：<RXMOP:MOTY=RXOCF;
	 * 
	 * @param datas
	 * @param tableName
	 * @return
	 */
	private List<String> handleType28(List<String> datas, String tableName) {
		List<String> inserts = new ArrayList<String>();
		List<String> vals = new ArrayList<String>();
		List<ColInfo> cols = new ArrayList<EricssonV1CmParserImp.ColInfo>();
		for (int i = 0; i < datas.size(); i++) {
			String line = datas.get(i);
			String trimLine = line.trim();
			if (trimLine.startsWith("MO") && trimLine.endsWith("SIG")) {
				cols.clear();
				vals.clear();
				List<ColInfo> tmp = splitColHeader(line);
				cols.addAll(tmp);
				vals.addAll(splitValues(datas.get(i + 1), cols, false));
				i += 1;
			} else if (trimLine.startsWith("SWVERREPL") && trimLine.endsWith("SWVERACT")) {
				String tmpLime1 = line.substring(line.indexOf("SWVERREPL"));
				String tmpLime2 = datas.get(i + 1).substring(line.indexOf("SWVERREPL"));
				List<ColInfo> tmp = splitColHeader(tmpLime1);
				cols.addAll(tmp);
				vals.addAll(splitValues(tmpLime2, tmp, false));
				i += 1;
			} else if (trimLine.startsWith("BSSWANTED") && trimLine.endsWith("NEGSTATUS")) {
				String tmpLime1 = line.substring(line.indexOf("BSSWANTED"));
				String tmpLime2 = datas.get(i + 1).substring(line.indexOf("BSSWANTED"));
				List<ColInfo> tmp = splitColHeader(tmpLime1);
				cols.addAll(tmp);
				vals.addAll(splitValues(tmpLime2, tmp, false));
				i += 1;
				inserts.add(createInsert(tableName, cols, vals));
				cols.clear();
				vals.clear();
			}
		}
		return inserts;
	}

	/**
	 * 处理节点：<RLCLP:CELL=ALL;
	 * 
	 * @param datas
	 * @param tableName
	 * @return
	 */
	private List<String> handleType29(List<String> datas, String tableName) {
		List<String> inserts = new ArrayList<String>();
		List<ColInfo> cols = null;
		for (int i = 0; i < datas.size(); i++) {
			String line = datas.get(i);
			if (cols == null && line.startsWith("CELL") && (line.endsWith("AHRTRX") || line.endsWith("CODECREST"))) {
				cols = splitColHeader(line);
			} else if (cols != null) {
				List<String> vals = splitValues(line, cols, false);
				inserts.add(createInsert(tableName, cols, vals));
			}
		}
		return inserts;
	}

	private static ColInfo findColInfoInList(List<ColInfo> cols, String name) {
		for (ColInfo ci : cols) {
			if (ci.colText.equalsIgnoreCase(name))
				return ci;
		}
		return null;
	}

	private String createInsert(String tableName, List<ColInfo> row, List<String> values) {
		sl.writeRow(tableName, row, values);
		return null;

	}

	@SuppressWarnings("unused")
	private boolean createTable(List<ColInfo> row, String tableName) {
		if (TABLES.contains(tableName)) {
			return true;
		}

		StringBuilder sql = new StringBuilder();

		sql.append("CREATE TABLE ").append(tableName).append(" (OMCID NUMBER,COLLECTTIME DATE,STAMPTIME DATE,BSC_NAME VARCHAR2(50),");

		for (int i = 0; i < row.size(); i++) {
			sql.append("\"").append(row.get(i).colText.toUpperCase()).append("\" NUMBER(13,3)");
			if (i != row.size() - 1) {
				sql.append(",");
			}
		}
		sql.append(")");

		try {
			CommonDB.executeUpdate(sql.toString());
			TABLES.add(tableName);
			return true;
		} catch (SQLException e) {
			if (e.getErrorCode() == 955) {
				TABLES.add(tableName);
				return true;
			} else {
				logger.error("创建表时异常，sql:" + sql, e);
			}
		}

		return false;
	}

	/**
	 * 列信息。 ColInfo
	 * 
	 * @author 陈思江 2010-3-23
	 */
	static class ColInfo {

		String colText;

		int startIndex;

		int endIndex;

		ColInfo(String colText, int startIndex, int endIndex) {
			this.colText = colText;
			this.startIndex = startIndex;
			this.endIndex = endIndex;
		}

		@Override
		public String toString() {
			return String.format("[colText:%s,startIndex:%s,endIndex:%s]", colText, startIndex, endIndex);
		}

		@Override
		public boolean equals(Object obj) {
			if(obj == null)
				throw new NullPointerException("the argument obj is null");
			ColInfo c = (ColInfo) obj;
			return c.colText.equalsIgnoreCase(colText);
		}
	}

	public static void main(String[] args) {
		EricssonV1CmParserImp parser = new EricssonV1CmParserImp();
		try {
			long curr = System.currentTimeMillis();
			// for (int i = 1; i <= 27; i++)
			// {
			// parser.parse("E:\\datacollector_path\\1014\\20101020\\ERBSC"
			// + i + ".log", 520, new
			// Timestamp(Util.getDate1("2010-10-20 00:00:00").getTime()), 989);
			// System.out.println("end " + i);
			// }
			parser.parse("C:\\Users\\ChenSijiang\\Desktop\\ERBSC18.log", 520, new Timestamp(Util.getDate1("2010-10-20 00:00:00").getTime()), 989);
			// parser.parse("e:\\资料\\解析\\ericsson\\WXBSC7", 520, new
			// Timestamp(new Date().getTime()));
			System.out.println(System.currentTimeMillis() - curr);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
