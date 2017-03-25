package parser.hw.mml.c;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import parser.AbstractStreamParser;
import sqlldr.SqlldrImpl;
import sqlldr.SqlldrInterface;
import task.CollectObjInfo;
import templet.TempletBase;
import templet.hw.cdma.mml.MmlCdmaTempletP;
import templet.hw.cdma.mml.MmlCdmaTempletP.Column;
import templet.hw.cdma.mml.MmlCdmaTempletP.Event;
import templet.hw.cdma.mml.MmlCdmaTempletP.Table;
import templet.hw.cdma.mml.MmlCdmaTempletP.Template;
import util.LogMgr;
import util.Util;
import framework.ConstDef;

public class MmlCdmaParser extends AbstractStreamParser {

	private static final Logger logger = LogMgr.getInstance().getSystemLogger();

	private String collecttime;

	private InputStream in = null;

	private PrintStream ps = null;

	private SqlldrInterface sqlldr = null;

	private List<Event> tmpEventList = null;

	private List<String/* 表名 */> NotToDatabaseTableList = null;

	private Map<String/* 表名 */, List<SortedMap<String, String>>/* key:字段名,value:字段值 */> tmpResultMap = null;

	private Map<String, List<List<String>>> rowMap = null;

	private String beginStr = "----";

	private String endStr1 = "结果个数";

	private String endStr2 = "END";

	private static final String rowArrangement = "row";

	private static final String columnArrangement = "column";

	@Override
	public boolean parseData() throws Exception {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void parse(InputStream in, OutputStream out) throws Exception {
		this.collecttime = Util.getDateString(new Timestamp(System.currentTimeMillis()));

		PrintStream ps = (PrintStream) out;
		this.in = in;
		this.ps = ps;

		TempletBase tBaseP = this.collectObjInfo.getParseTemplet();
		if (!(tBaseP instanceof MmlCdmaTempletP))
			return;
		MmlCdmaTempletP templetP = (MmlCdmaTempletP) tBaseP;
		List<Template> templets = templetP.templets;
		for (Template template : templets) {
			Map<Integer, Event> eventMap = template.eventMap;

			// 获取最小运行周期period
			int periodTem = getTaskPeriodFromTemplate(eventMap);
			// 获取任务执行周期period
			int periodCfg = getTaskPeriodFromTaskCfg();
			if (periodTem != periodCfg) {
				throw new Exception("模板中配置的最小执行周期不等于任务配置的执行周期，请检查");
			}

			Iterator<Integer> it = eventMap.keySet().iterator();
			while (it.hasNext()) {
				Event event = eventMap.get(it.next());

				// 是否执行命令（startTime不等于空的事件）
				String startTime = event.startTime;
				if (startTime != null && startTime.trim().length() != 0) {
					int hour = Integer.parseInt(startTime.substring(0, startTime.indexOf(":")));
					int minute = Integer.parseInt(startTime.substring(startTime.indexOf(":") + 1));

					Calendar startTimeCal = getCalendar(hour, minute);
					Calendar endTimeCal = getCalendar(hour, minute + periodTem);

					Calendar runTimeCal = Calendar.getInstance();
					runTimeCal.setTime(this.getCollectObjInfo().getLastCollectTime());

					boolean betweenFlag = runTimeCal.compareTo(startTimeCal) == 1 && runTimeCal.compareTo(endTimeCal) == -1;
					boolean equalFlag = runTimeCal.compareTo(startTimeCal) == 0 || runTimeCal.compareTo(endTimeCal) == 0;
					// 不在周期内，跳过
					if (!betweenFlag && !equalFlag) {
						// 记录下不需要入库的表（没到点）
						recordNotToDatabaseTableList(event);
						continue;
					}
				}

				// 在执行周期内
				if (checkEvent(event)) {
					// 初步检查通过
					if (event.engineEventId != null && event.engineEventId > 0) {
						if (eventMap.get(event.engineEventId) == null) {
							throw new NullPointerException("engineEventId 不存在");
						}
					}

					// 包含参数
					if (event.commandTemplate.contains("?") && event.commandTemplate.contains("=")) {
						// 常量参数,并且是一个参数,example:<para>SSTP?=AMUO,PMUO,SMUO,RPUO</para>
						if (!event.para.contains(".") && !event.para.contains(";")) {
							conExecCommand(event);
							continue;
						}

						// 变量参数,example:<para>OPC?=CLT_MML_N70PC_HW.SOURCE_SIGNAL_CODE</para>
						List<String> paraList = split(event.para, ';');// 分拆字符串 “;”

						// 获取表名
						String tabName = getTableName(paraList);

						// key:参数名 value:字段名
						Map<String, String> paraMap = getParaMap(paraList);

						// 查询tabName是否已经有值，没有先挂起，保存起来，放在最后执行
						if (this.tmpResultMap.get(tabName) == null) {
							if (this.tmpEventList == null)
								this.tmpEventList = new ArrayList<Event>();
							this.tmpEventList.add(event);
							continue;
						}

						// 批量执行命令(tabName已有，可以开始解析)
						batchExecCommand(event, tabName, paraMap, eventMap);

					}
					// 不包含参数
					else {
						execCommand(event, null);
					}
				}
			}
			// 处理tmpEventList
			hanldeTmpEventList(eventMap);
		}

		// sqlldr操作 入库
		doSqlldr();

		// clean
		this.tmpResultMap = null;
		this.tmpEventList = null;
		this.rowMap = null;
		this.sqlldr = null;
		this.NotToDatabaseTableList = null;
	}

	private Calendar getCalendar(int hour, int minute) {
		Calendar startTimeCal = Calendar.getInstance();
		startTimeCal.setTime(this.getCollectObjInfo().getLastCollectTime());
		startTimeCal.set(startTimeCal.HOUR, hour);
		startTimeCal.set(startTimeCal.MINUTE, minute);
		return startTimeCal;
	}

	/**
	 * 记录下不需要入库的表（没到点）
	 * 
	 * @param event
	 */
	private void recordNotToDatabaseTableList(Event event) {
		NotToDatabaseTableList = new ArrayList<String>();
		for (Table table : event.tableList) {
			NotToDatabaseTableList.add(table.tableName);
		}
	}

	/**
	 * 从任务配置中获取任务执行周期(单位：分钟)
	 * 
	 * @return
	 */
	private int getTaskPeriodFromTaskCfg() {
		int periodCfg = 0;
		switch (this.collectObjInfo.getPeriod()) {
			case ConstDef.COLLECT_PERIOD_10MINUTE :
				periodCfg = 10;
				break;
			case ConstDef.COLLECT_PERIOD_MINUTE_QUARTER :
				periodCfg = 15;
				break;
			case ConstDef.COLLECT_PERIOD_MINUTE_HALFHOUR :
				periodCfg = 30;
				break;
			case ConstDef.COLLECT_PERIOD_5MINUTE :
				periodCfg = 5;
				break;
			case ConstDef.COLLECT_PERIOD_HOUR :
				periodCfg = 60;
				break;
			case ConstDef.COLLECT_PERIOD_4HOUR :
				periodCfg = 4 * 60;
				break;
			case ConstDef.COLLECT_PERIOD_HALFDAY :
				periodCfg = 12 * 60;
				break;
			case ConstDef.COLLECT_PERIOD_WEEK :
				periodCfg = 7 * 24 * 60;
				break;
			case ConstDef.COLLECT_PERIOD_DAY :
				periodCfg = 24 * 60;
				break;
			case ConstDef.COLLECT_PERIOD_MONTH :
				periodCfg = 30 * 24 * 60;
				break;
			case ConstDef.COLLECT_PERIOD_ONE_MINUTE :
				periodCfg = 1;
				break;
			default :
				log.debug(this.collectObjInfo.getTaskID() + " : without period type(" + this.collectObjInfo.getPeriod() + ").");
		}
		return periodCfg;
	}

	/**
	 * 从模板中获取任务执行最小时间
	 * 
	 * @param eventMap
	 * @return
	 */
	private int getTaskPeriodFromTemplate(Map<Integer, Event> eventMap) {
		Iterator<Integer> it0 = eventMap.keySet().iterator();
		int minVal = 0;
		while (it0.hasNext()) {
			Event event = eventMap.get(it0.next());
			if (minVal == 0) {
				minVal = event.period;
				continue;
			}
			if (minVal > event.period) {
				minVal = event.period;
				continue;
			}
		}
		return minVal;
	}

	private void conExecCommand(Event event) throws Exception {
		String paraName = event.para.substring(0, event.para.indexOf("="));
		String paraValue = event.para.substring(event.para.indexOf("=") + 1, event.para.length());
		List<String> valueList = split(paraValue, ',');
		Map<String, String> backFillMap = null;
		for (String val : valueList) {
			event.commandTemplate = event.commandTemplate.replace(paraName, val.trim());
			backFillMap = new HashMap<String, String>();
			backFillMap.put(paraName, val.trim());
			// 回填字段
			execCommand(event, backFillMap);
		}
	}

	/**
	 * 入库
	 * 
	 * @throws Exception
	 */
	private void doSqlldr() throws Exception {
		// 开始准备入库
		logger.debug(this.collectObjInfo.getTaskID() + " - 开始准备入库 " + this.collectObjInfo.getLastCollectTime());
		// 所有的记录 rows
		this.rowMap = new HashMap<String, List<List<String>>>();

		// 预处理
		preparedSqlldr();

		// 初始化
		this.sqlldr.initSqlldr();

		// 写入sqlldr文件
		writeRows();

		// 开始执行入库
		logger.debug(this.collectObjInfo.getTaskID() + " - 开始执行入库 " + this.collectObjInfo.getLastCollectTime());
		this.sqlldr.runSqlldr();
	}

	/**
	 * 批量执行命令
	 * 
	 * @param event
	 * @param tabName
	 * @param paraMap
	 * @throws IOException
	 */
	private void batchExecCommand(Event event, String tabName, Map<String, String> paraMap, Map<Integer, Event> eventMap) throws Exception {
		for (SortedMap<String, String> map : this.tmpResultMap.get(tabName)) {
			Iterator<String> paraIter = paraMap.keySet().iterator();
			Map<String, String> backFillMap = new HashMap<String, String>();
			while (paraIter.hasNext()) {
				String key = paraIter.next().toString();
				event.commandTemplate.replace("=" + key, "=" + map.get(paraIter.next()));
				// 把参数作为回填字段 去掉"?"
				backFillMap.put(key.substring(0, key.length() - 1), map.get(paraIter.next()));
			}

			// engineEventId的参数也作为回填字段
			String paraKey = getEngineEventParaKey(event, eventMap);
			if (paraKey != null)
				backFillMap.put(paraKey, map.get(paraKey));

			// 回填字段
			execCommand(event, backFillMap);
		}
	}

	/**
	 * 获取event的engineEvent的paraKey
	 * 
	 * @param event
	 * @param eventMap
	 * @return
	 */
	private String getEngineEventParaKey(Event event, Map<Integer, Event> eventMap) {
		Event engineEvent = eventMap.get(event.engineEventId);
		String para = engineEvent.para;
		if (para == null || para.trim().length() == 0)
			return null;
		String paraKey = para.substring(0, para.indexOf("="));
		paraKey = paraKey.substring(0, paraKey.length() - 1);
		return paraKey;
	}

	/**
	 * 处理挂起的event
	 * 
	 * @param eventMap
	 * @throws Exception
	 */
	private void hanldeTmpEventList(Map<Integer, Event> eventMap) throws Exception {
		for (Event event : tmpEventList) {
			// 变量参数,example:<para>OPC?=CLT_MML_N70PC_HW.SOURCE_SIGNAL_CODE</para>
			List<String> paraList = split(event.para, ';');// 分拆字符串 “;”

			// 获取表名
			String tabName = getTableName(paraList);

			// key:参数名 value:字段名
			Map<String, String> paraMap = getParaMap(paraList);

			if (this.tmpResultMap.get(tabName) == null) {
				throw new NullPointerException("模板中表名" + tabName + "对应的event不存在");
			}

			// 批量执行命令
			batchExecCommand(event, tabName, paraMap, eventMap);
		}
	}

	/**
	 * paraMap key:参数名 value:字段名
	 * 
	 * @param paraList
	 * @return
	 */
	private Map<String, String> getParaMap(List<String> paraList) {
		Map<String, String> paraMap = new HashMap<String, String>();
		for (String para : paraList) {
			String paraName = para.substring(0, para.indexOf("="));
			String paraValue = para.substring(para.indexOf("=") + 1, para.length());
			String colName = paraValue.substring(paraValue.indexOf(".") + 1, paraValue.length());
			paraMap.put(paraName, colName);
		}
		return paraMap;
	}

	/**
	 * 获取表名
	 * 
	 * @param paraList
	 * @return
	 */
	private String getTableName(List<String> paraList) {
		String paraValue = paraList.get(0).substring(paraList.get(0).indexOf("=") + 1, paraList.get(0).length());
		return split(paraValue, '.').get(0);
	}

	/**
	 * 一行一行的写入数据
	 */
	private void writeRows() {
		Iterator<String> iterator = this.rowMap.keySet().iterator();
		while (iterator.hasNext()) {
			for (List<String> row : this.rowMap.get(iterator.next()))
				this.sqlldr.writeRows(row, iterator.next());
		}
	}

	/**
	 * 预处理 准备sqlldr
	 * 
	 * @return
	 */
	private void preparedSqlldr() throws Exception {
		this.sqlldr = new SqlldrImpl(this.collectObjInfo.getTaskID(), this.collectObjInfo.getDevInfo().getOmcID(),
				this.collectObjInfo.getLastCollectTime(), "C_HW_MML");
		// 重组数据 转成Map<String, List<String>> tableCols
		Map<String, List<String>> tableCols = new HashMap<String, List<String>>();
		Iterator<String> iter = this.tmpResultMap.keySet().iterator();
		while (iter.hasNext()) {
			String tableName = iter.next();

			// 过滤掉需要入库的表
			if (this.NotToDatabaseTableList.contains(tableName)) {
				continue;
			}

			List<String> colmunList = new ArrayList<String>();
			List<List<String>> rowList = null;

			// 初始化 加上常用字段
			colmunList.add("OMCID");
			colmunList.add("CITY_ID");
			colmunList.add("BSC_ID");
			colmunList.add("COLLECTTIME");
			colmunList.add("STAMPTIME");

			for (int m = 0; m < this.tmpResultMap.get(tableName).size(); m++) {
				List<String> row = new ArrayList<String>();

				// 加上常用字段值
				row.add(String.valueOf(this.collectObjInfo.getDevInfo().getOmcID()));
				row.add(String.valueOf(this.collectObjInfo.getDevInfo().getCityID()));
				row.add(String.valueOf(this.collectObjInfo.getGroupId()));// groupid字段作为bsc_id
				row.add(this.collecttime);
				row.add(Util.getDateString(this.collectObjInfo.getLastCollectTime()));

				SortedMap<String, String> map = this.tmpResultMap.get(tableName).get(m);
				if (m == 0) {
					Iterator<String> itera = map.keySet().iterator();
					while (itera.hasNext()) {
						if (colmunList == null)
							colmunList = new ArrayList<String>();
						colmunList.add(itera.next());
					}
				}
				Iterator<String> itera0 = map.keySet().iterator();
				while (itera0.hasNext()) {
					row.add(map.get(itera0.next()));
				}
				if (rowList == null)
					rowList = new ArrayList<List<String>>();
				rowList.add(row);
			}
			this.rowMap.put(tableName, rowList);
			tableCols.put(tableName, colmunList);
		}
		this.sqlldr.setTableCols(tableCols);
	}

	/**
	 * 执行命令
	 * 
	 * @param event
	 * @throws IOException
	 */
	private void execCommand(Event event, Map<String, String> backFillMap) throws Exception {
		// 输入命令
		String command = event.commandTemplate;
		logger.info("任务：[" + this.collectObjInfo.getTaskID() + "]，发送命令:" + command);
		this.ps.println(command);
		this.ps.flush();

		// 解析流
		String line = null;
		BufferedReader reader = getReader(this.in);
		List<String> lineList = null;
		String title = null;
		while ((line = reader.readLine()) != null) {
			line = line.trim();
			if (line.contains(this.beginStr))// 查询结果开始
			{
				if (lineList == null)
					lineList = new ArrayList<String>();
				// 抬头
				if (event.arrangement.equals("row"))
					title = reader.readLine();
				// 内容列表
				while ((line = reader.readLine()) != null) {
					if (line.contains(this.endStr1) || line.contains(this.endStr2))// 查询结果结束
						break;
					lineList.add(line);
				}
			}
			if (line.contains(this.endStr1) || line.contains(this.endStr2))// 查询结果结束
				break;

		}

		// 解析 lineList
		parseLines(event, lineList, title, backFillMap);
	}

	/**
	 * 解析 lineList 组装成 tmpResultMap
	 * 
	 * @param event
	 * @param lineList
	 * @param title
	 */
	private void parseLines(Event event, List<String> lineList, String title, Map<String, String> backFillMap) throws Exception {
		// 行排列
		if (event.arrangement.equals(rowArrangement)) {
			for (Table table : event.tableList) {
				List<Column> columnList = table.columnList;

				SortedMap<String, String> tmpMap = null;
				List<SortedMap<String, String>> list = null;
				if (this.tmpResultMap == null)
					this.tmpResultMap = new HashMap<String, List<SortedMap<String, String>>>();
				for (String lineStr : lineList) {
					tmpMap = new TreeMap<String, String>();
					if (list == null)
						list = new ArrayList<SortedMap<String, String>>();
					for (int n = 0; n < columnList.size(); n++) {
						Column column = columnList.get(n);
						if (n == (columnList.size() - 1)) {
							tmpMap.put(column.name, lineStr.substring(title.indexOf(column.from)).trim());
						} else {
							tmpMap.put(column.name, lineStr.substring(title.indexOf(column.from), title.indexOf(columnList.get(n + 1).from)).trim());
						}
					}
					// 添加上回填字段
					if (backFillMap != null) {
						tmpMap.putAll(backFillMap);
					}
					list.add(tmpMap);
				}
				this.tmpResultMap.put(table.tableName, list);
			}
			// 列排列
		} else if (event.arrangement.equals(columnArrangement)) {
			for (Table table : event.tableList) {
				List<Column> columnList = table.columnList;

				SortedMap<String, String> tmpMap = null;
				List<SortedMap<String, String>> list = new ArrayList<SortedMap<String, String>>();;
				if (this.tmpResultMap == null)
					this.tmpResultMap = new HashMap<String, List<SortedMap<String, String>>>();
				for (String lineStr : lineList) {
					tmpMap = new TreeMap<String, String>();
					int index = lineStr.indexOf("=");
					if (index == -1)
						continue;
					String columnName = lineStr.substring(0, index).trim();
					String value = lineStr.substring(index + 1).trim();
					for (int n = 0; n < columnList.size(); n++) {
						Column column = columnList.get(n);
						if (column.from.equals(columnName)) {
							tmpMap.put(column.name, value);
							break;
						}
					}
				}
				// 添加上回填字段
				if (backFillMap != null) {
					tmpMap.putAll(backFillMap);
				}
				list.add(tmpMap);
				this.tmpResultMap.put(table.tableName, list);
			}
		}
	}

	/**
	 * 实现String.split(regex)方法
	 * 
	 * @param string
	 * @param split
	 * @return
	 */
	private List<String> split(String string, char regex) {
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
	 * 检查Event
	 * 
	 * @param event
	 * @return
	 * @throws Exception
	 */
	public boolean checkEvent(Event event) throws Exception {
		if (event.commandTemplate == null || "".equals(event.commandTemplate.trim())) {
			throw new NullPointerException("commandTemplate 为空");
		}
		if (event.commandTemplate.contains("?") && event.commandTemplate.contains("=")) {
			if (event.para == null || "".equals(event.para.trim())) {
				throw new NullPointerException("para 为空");
			}
		}
		if (event.period == null || event.period == 0) {
			throw new NullPointerException("period 为空");
		}
		if (event.arrangement == null || "".equals(event.arrangement.trim())) {
			throw new NullPointerException("arrangement 为空");
		}
		return true;
	}

	/**
	 * 获取reader
	 * 
	 * @param in
	 * @return
	 * @throws IOException
	 */
	private BufferedReader getReader(InputStream in) throws Exception {
		String encode = this.collectObjInfo.getDevInfo().getEncode();
		if (encode == null || encode.equals("")) {
			return new BufferedReader(new InputStreamReader(in));
		}

		return new BufferedReader(new InputStreamReader(in, encode));
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		MmlCdmaParser parser = new MmlCdmaParser();
		parser.collectObjInfo = new CollectObjInfo(888);
		parser.collectObjInfo.setPeriod(12);
		parser.getTaskPeriodFromTaskCfg();
	}

}
