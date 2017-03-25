package parser.siemens.pm;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import parser.Parser;
import task.CollectObjInfo;
import task.DevInfo;
import templet.siemens.GRC90PMTempletP;
import util.Util;
import distributor.DistributeTemplet;
import distributor.TableItem;
import distributor.DistributeTemplet.TableTemplet;

/**
 * 西门子性能解析RC90
 * 
 * @author litp
 * @since 3.0
 * @see GRC90PMTempletP
 */
public class GRC90ASCII extends Parser {

	private String stamptime = null;

	private long taskId = 0;

	// 对整条记录进行匹配 (4,5),(时间),{里面},(BSS:47/SCANGPRS:2 60 )后面
	private final String regex = ".+?(\\(\\d+,\\d+\\))\\s+([^+]+)[^{]+?\\{([^}]+?)\\}\\s+\\d+\\s+.+?\\s+\\d+\\s+(.+)";

	// 对邻区数据进行匹配 上面的第四组 (数字，组数) 多组(aa {数据}) 其它
	private final String cntRegex = "(\\d+)((?:[^{]+\\{(?:[^}]+)\\})+)(.+)";

	// 对子邻区进行匹配 上面每二组 小括号中的(数字)
	private final String subRegex = "[^{]+\\{([^}]+)\\}";

	private Matcher matcher = null;

	private Matcher cntMathcher = null;

	private Matcher subMatcher = null;

	private static final String COUNT_KEYS = "(6,0),(6,1),(6,2)";

	public GRC90ASCII() {
		matcher = Pattern.compile(regex).matcher("");
		cntMathcher = Pattern.compile(cntRegex).matcher("");
		subMatcher = Pattern.compile(subRegex).matcher("");
	}

	@Override
	public boolean parseData() {
		taskId = collectObjInfo.getTaskID();
		stamptime = Util.getDateString(collectObjInfo.getLastCollectTime());
		boolean flag = false;
		String parseFileName = getFileName();
		File file = new File(parseFileName);
		BufferedReader bf = null;
		if (!file.exists()) {
			log.error("Task-" + taskId + "指定文件不存在!" + parseFileName);
			return flag;
		}
		try {
			bf = new BufferedReader(new FileReader(file));
			String lineData = null;
			while ((lineData = bf.readLine()) != null) {
				if (lineData.equals(""))
					continue;
				parseLineData(lineData);
			}
			flag = true;
		} catch (IOException e) {
			flag = false;
			log.error("Task-" + taskId + "解析文件时发生错误！" + parseFileName, e);
		} finally {
			try {
				if (bf != null)
					bf.close();
			} catch (IOException e) {
			}
		}
		return flag;
	}

	/**
	 * 根据表名获取该表的文件流
	 * 
	 * @param table
	 *            表名
	 * @return
	 */
	private TableItem getTableItem(String table) {
		TableItem t = null;
		int tableIndex = 0;
		// 获取所有表模板
		Collection<TableTemplet> tableTemplets = ((DistributeTemplet) collectObjInfo.getDistributeTemplet()).tableTemplets.values();
		for (TableTemplet tableTemplet : tableTemplets) {
			// 根据表名获取模板，从而获取模板索引
			if (tableTemplet.tableName.equalsIgnoreCase(table)) {
				tableIndex = tableTemplet.tableIndex;
				break;
			}
		}
		// 根据模板索引获取文件名柄
		Map<Integer, TableItem> tableItems = ((DistributeTemplet) collectObjInfo.getDistributeTemplet()).tableItems;
		t = tableItems.get(tableIndex);
		return t;
	}

	private void parseLineData(String lineData) {
		matcher.reset(lineData);
		if (!matcher.find()) {
			return;
		}
		try {
			String countKey = matcher.group(1).trim();
			// 根据countKey查询表名，如果表名为空，那么这条数据就不需要解析
			String table = getTable(countKey);
			if (table == null) {
				return;
			}
			//
			TableItem tableItem = getTableItem(table);
			FileWriter fileWriter = tableItem.fileWriter;
			if (fileWriter == null) {
				return;
			}
			//
			Date now = new Date();
			StringBuilder values = new StringBuilder();
			values.append(collectObjInfo.getDevInfo().getOmcID()).append(";");
			values.append(Util.getDateString(now)).append(";");
			values.append(stamptime).append(";");
			values.append(countKey).append(";");
			values.append(matcher.group(2)).append(";");
			String lastFour = matcher.group(3).trim();
			if (lastFour == null) {
				return;
			}
			String[] fours = lastFour.split("\\s+");
			int len = 0;
			if ((len = fours.length) < 2) {
				return;
			}
			// 大括号中的数字的数量不一样时，要加的列也不一样
			if (len == 2) {
				values.append(fours[0]).append(";");
				skipNull(values, 3);
			} else {
				for (int i = 0; i < len; i++) {
					values.append(fours[i]).append(";");
				}
				skipNull(values, 4 - len);
			}
			// 是否有字count
			if (matcher.group(4).contains("{")) {
				cntMathcher.reset(matcher.group(4));
				if (cntMathcher.find()) {
					int group2 = Integer.parseInt(cntMathcher.group(1));
					int j = group2 + 1;
					String[] ds = cntMathcher.group(3).trim().split("\\s+");
					int single = (ds.length - j) / group2;// 如果有多个子bts，那么就算出单个bts的长度
					subMatcher.reset(cntMathcher.group(2));
					while (subMatcher.find()) {

						int destLen = 0;
						String[] destFour = subMatcher.group(1).split("\\s+");
						// DEST_BSC1,DEST_BTS1,DEST_SECTOR1,DEST_CARRIER1
						if (destFour == null || (destLen = destFour.length) < 2) {
							continue;
						}
						StringBuilder subVal = new StringBuilder();
						skipNull(subVal, 4);
						// 大括号中的数字的数量不一样时，要加的列也不一样
						if (destLen == 2) {
							// 新修改10/08/06
							if (countKeyExists(countKey)) {
								subVal.append(destFour[0]).append(";");
								subVal.append(destFour[1]).append(";");
								subVal.append(255).append(";;");
							} else {
								// 原来的方式
								subVal.append(destFour[0]).append(";");
								skipNull(subVal, 3);
							}
						} else {

							for (int i = 0; i < destLen; i++) {
								subVal.append(destFour[i]).append(";");
							}
							skipNull(subVal, 4 - len);
						}
						// 新增(当count_key为(6,0),(6,1),(6,2),)就加上tgtbts {49
						// 1}中的49,1到TGTBSC，TGTBTS
						if (destLen == 2 && countKeyExists(countKey)) {
							subVal.append(destFour[0]).append(";");
							subVal.append(destFour[1]).append(";");
						} else {
							skipNull(subVal, 2);
						}
						//
						// skipNull(subVal, 2);以前的方式
						int max = j + single;
						for (; j < max; j++) {
							subVal.append(ds[j]).append(";");
						}
						tableItem.recordCounts += 1;
						fileWriter.write(values.toString() + subVal.toString() + "\n");
					}
				}
			} else {
				skipNull(values, 7);
				String[] ds = matcher.group(4).split("\\s+");
				// 因为真正数据要跳过四个数字
				for (int i = 4; i < ds.length; i++) {
					// 从COUNT1开始
					values.append(ds[i]).append(";");
				}
				tableItem.recordCounts += 1;
				fileWriter.write(values.toString() + "\n");
			}
			fileWriter.flush();
		} catch (Exception e) {
			log.error("Task-" + taskId + "数据解析错误:" + lineData, e);
		}
	}

	private boolean countKeyExists(String countKey) {
		boolean flag = false;
		if (COUNT_KEYS.contains(countKey))
			flag = true;
		return flag;
	}

	private StringBuilder skipNull(StringBuilder sb, int len) {
		for (int i = 0; i < len; i++) {
			sb.append(";");
		}
		return sb;
	}

	// 根据countKey获得表名
	private String getTable(String countKey) {
		String table = null;
		table = ((GRC90PMTempletP) collectObjInfo.getParseTemplet()).getTableByCountKey(countKey);
		return table;
	}

	public static void main(String[] args) {
		GRC90ASCII g = new GRC90ASCII();
		CollectObjInfo c = new CollectObjInfo(1122);
		DevInfo d = new DevInfo();
		d.setOmcID(99);
		c.setDevInfo(d);
		c.setLastCollectTime(new Timestamp(new Date().getTime()));
		g.collectObjInfo = c;
		g.fileName = "C:\\Documents and Settings\\ChenSijiang\\桌面\\BR90_23.20110810130000+0000.20110810140000+0000-000.ASCII";
		g.parseData();
	}

}
