package parser.xparser.tag;

import java.io.File;
import java.io.FileReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import parser.Parser;
import parser.xparser.IllegalTagException;
import task.CollectObjInfo;
import templet.xparser.GenericTemplet;
import util.ColumnType;
import util.CommonDB;
import util.LogMgr;
import util.SqlLdrWriter;
import util.SqlldrRunner;
import util.Util;
import framework.SystemConfig;

public class GenericParser extends Parser {

	/**
	 * 被解析的文件
	 */
	private String source;

	/**
	 * 解析模板配置信息
	 */
	private Definition def;

	/**
	 * 采集任务的信息
	 */
	// private CollectObjInfo gatherObjInfo;
	// 剩余未解析的字符串
	private String remainingData = "";

	// <表名,对应的sqlldr写入器>
	private Map<String, SqlLdrWriter> writers = new HashMap<String, SqlLdrWriter>();

	// <表名，表的所有列<列的信息>>
	private Map<String, List<PropertyElement>> scopeMap;

	// 存放sqlldr文件名 <表名,sqlldr文件名>
	private Map<String, String> sqlldrFilenames = new Hashtable<String, String>();

	private final static int BUFFER_SIZE = 65536;

	private static Logger logger = LogMgr.getInstance().getSystemLogger();

	public GenericParser() {
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public Definition getDefinition() {
		return def;
	}

	public void setDefinition(Definition def) {
		this.def = def;
	}

	/**
	 * 开始解析数据，并生成sqlloader文件
	 * 
	 * @throws Exception
	 */
	public void parse() throws Exception {
		source = this.getFileName();

		FileReader reader = new FileReader(source);

		char[] buff = new char[BUFFER_SIZE];

		int len = 0;
		while ((len = reader.read(buff)) > 0) {
			parseData(buff, len);
		}

		// 处理剩下没处理在数据
		remainingHandle();

		Collection<SqlLdrWriter> tmpWriters = writers.values();
		if (tmpWriters != null) {
			for (SqlLdrWriter s : tmpWriters) {
				if (s == null)
					continue;
				s.commit();
				s.dispose();
			}
		}

		if (reader != null) {
			reader.close();
		}

		writers.clear();
		remainingData = "";

		// 以下开始执行sqlldr入库
		CfgElement cfg = def.getCfgElement();
		Iterator<Entry<String, String>> it = sqlldrFilenames.entrySet().iterator();
		while (it.hasNext()) {
			Entry<String, String> entry = it.next();
			String name = entry.getValue().substring(0, entry.getValue().lastIndexOf("."));
			SqlldrRunner runner = new SqlldrRunner(cfg.getService(), cfg.getUsername(), cfg.getPassword(), name + ".clt", name + ".bad", name
					+ ".log", cfg.getSkip());
			runner.runSqlldr(collectObjInfo);
		}
		sqlldrFilenames.clear();
		remainingData = "";
	}

	private void parseData(char[] data, int length) throws IllegalTagException, Exception {
		remainingData += new String(data, 0, length);

		OwnerElement owner = def.getProcessElement().findOwner(new File(source).getName(), collectObjInfo.getLastCollectTime());
		if (owner == null) {
			throw new IllegalTagException("缺少<owner>标签");
		}

		// 在数据块中寻找宿主以及宿主对应的数据块
		Object ownerReturn = owner.apply(remainingData);
		if (ownerReturn == null) {
			return;
		}
		String ownerReturnStr = ownerReturn.toString();
		String[] splitOwner = ownerReturnStr.split(":", 2);
		String ownerName = splitOwner[0]; // 宿主名

		String matchDatas = splitOwner[1]; // 匹配到的隶属于此宿主名的数据块
		// 剩余的数据块(没有被本次操作匹配到的数据块,到下次一并解析)
		remainingData = remainingData.substring(remainingData.indexOf(matchDatas) + matchDatas.length());

		if (def.getPropertiesByTable(ownerName).isStore())
			// 创建sqlldr文件写入器
			if (!createWriters(ownerName)) {
				throw new Exception("创建sqlldr文件失败，解析中止。");
			}

		RecordElement record = owner.getRecordByName(ownerName, new File(getFileName()).getName(), collectObjInfo.getLastCollectTime());
		if (record == null) {
			Tag[] mchilds = def.getMapping().getChild();
			for (Tag t : mchilds) {
				PropertiesElement pe = (PropertiesElement) t;
				record = owner.getRecordByName(pe.getTable(), new File(getFileName()).getName(), collectObjInfo.getLastCollectTime());
				if (record != null) {
					break;
				}
			}
		}
		if (record == null)
			throw new IllegalTagException("缺少<record>标签");

		// 获取匹配的记录
		RuleElement matchRule = record.getMatchRule();
		if (matchRule == null)
			throw new IllegalTagException("record标签中缺少match-rule属性");

		String[] matchRecords = null;
		Object obj = null;
		obj = matchRule.apply(matchDatas); // 此处返回一个数组
		if (obj != null) {
			if (obj.getClass().isArray()) {
				matchRecords = (String[]) obj;
			} else
				matchRecords = new String[]{obj.toString()};

			int len = matchRecords.length;
			// 没有数据
			if (len == 0)
				return;

			// 挖掘数据
			String remaining = digRecord(record, matchRecords);
			remainingData = remaining + remainingData;

			if (writers.containsKey(ownerName))
				writers.get(ownerName).commit();
		}
	}

	/**
	 * 处理剩下没处理在数据
	 */
	private void remainingHandle() {
		OwnerElement owner = def.getProcessElement().findOwner(new File(source).getName(), collectObjInfo.getLastCollectTime());
		if (owner == null) {
			remainingData = "";
			return;
		}

		Object ownerReturn = owner.apply(remainingData);
		if (ownerReturn == null) {
			remainingData = "";
			return;
		}

		String ownerName = ownerReturn.toString(); // 宿主名

		RecordElement record = owner.getRecordByName(ownerName, new File(getFileName()).getName(), collectObjInfo.getLastCollectTime());
		if (record == null) {
			Tag[] mchilds = def.getMapping().getChild();
			for (Tag t : mchilds) {
				PropertiesElement pe = (PropertiesElement) t;
				record = owner.getRecordByName(pe.getTable(), new File(getFileName()).getName(), collectObjInfo.getLastCollectTime());
				if (record != null) {
					break;
				}
			}
		}
		if (record == null) {
			remainingData = "";
			return;
		}

		digRecord(record, remainingData);

		remainingData = "";

	}

	/**
	 * 分发一条数据
	 */
	private void distribute(String ownerName) throws IllegalTagException, Exception {
		String splitSign = def.getCfgElement().getSign();
		if (def.getMapping().hasChild()) {
			PropertiesElement properties = def.getPropertiesByTable(ownerName);
			if (properties.hasChild()) {
				List<PropertyElement> lst = new ArrayList<PropertyElement>();
				List<PropertyElement> gLst = scopeMap.get("global");
				if (gLst != null) {
					List<PropertyElement> clst = new ArrayList<PropertyElement>();
					for (PropertyElement pe : gLst) {
						if (pe.getTableName().equals(ownerName))
							continue;

						clst.add(pe);
					}

					if (clst != null)
						lst.addAll(clst);
				}

				List<PropertyElement> oLst = scopeMap.get(ownerName);
				if (oLst != null)
					lst.addAll(oLst);

				try {
					lst.addAll(asList(properties.getChild()));
				} catch (Exception e) {
					throw new IllegalTagException("<properties>配置有误");
				}

				StringBuilder sb = new StringBuilder();
				int length = lst.size();
				if (length > 0) {
					for (int i = 0; i < length - 1; i++) {
						PropertyElement pe = lst.get(i);
						if (pe.isVar()) {
							pe.setValue(getSystemVar(pe.getVar()));
						}
						sb.append(pe.getValue().trim()).append(splitSign);
					}
					PropertyElement last = lst.get(length - 1);
					if (last.isVar()) {
						last.setValue(getSystemVar(last.getVar()));
					}
					sb.append(last.getValue().trim());

					if (writers.containsKey(ownerName))
						// writers.get(ownerName).write(sb.toString(), false);
						writers.get(ownerName).write(replceStr(sb.toString()), false);
				}
			} else {
				throw new IllegalTagException("<properties>标签中无<property>标签");
			}
		} else {
			throw new IllegalTagException("<mapping>标签中无<properties>标签");
		}
	}

	private List<PropertyElement> asList(Tag[] tags) {
		List<PropertyElement> lst = new ArrayList<PropertyElement>();
		for (Tag tag : tags) {
			lst.add((PropertyElement) tag);
		}

		return lst;
	}

	/**
	 * 挖掘数据
	 * 
	 * @param e
	 * @param records
	 */
	private String digRecord(RecordElement e, String[] records) {
		int len = records.length;
		int tmplength = len - 1;
		// 当只有一条记录传入近来的时候,全部处理掉
		if (len == 1) {
			tmplength = 1;
		}
		// 最后一行不解析,与下次数据一起解析
		for (int i = 0; i < tmplength; i++) {
			String strRecord = records[i];
			if (Util.isNull(strRecord))
				continue;

			// 将匹配好的记录传递给dig
			digRecord(e, strRecord);
		}

		if (len == 1) {
			return "";
		} else
			return records[len - 1];
	}

	private void digRecord(RecordElement e, String record) {
		try {
			// 将匹配好的记录传递给dig
			RuleElement digRule = e.getDigRule();
			digRule.apply(record);

			distribute(e.getOwnerName());
		} catch (IllegalTagException ie) {
			logger.error(String.format("跳过此数据，原因：%s，数据为：\n%s\n", ie.getMessage(), record));
		} catch (Exception ex) {
			logger.error(String.format("跳过此数据，原因：挖掘此数据时失败，数据为：\n%s\n", record));
			ex.printStackTrace();
		}
	}

	/**
	 * 创建sqlldr文件写入器。
	 * 
	 * @param tableName
	 * @return
	 */
	private boolean createWriters(String tableName) {
		if (!writers.containsKey(tableName)) {
			try {
				String fileName = SystemConfig.getInstance().getCurrentPath() + File.separator + collectObjInfo.getTaskID() + "_"
						+ Util.getDateString_yyyyMMddHHmmssSSS(new Date()) + ".txt"; // 构造文件名
				sqlldrFilenames.put(tableName, fileName);
				SqlLdrWriter writer = new SqlLdrWriter(fileName, def.getCfgElement().getBacklogCount());
				writer.setCharset(def.getCfgElement().getCharset());// add by
				// liuwx on
				// 2010-03-15
				writer.setFileName(fileName);
				writer.setTableName(tableName);

				List<ColumnType> cols = new ArrayList<ColumnType>();

				addColumnTypeList("global", tableName, cols);
				addColumnTypeList(tableName, tableName, cols);

				PropertiesElement propertiesElement = def.getPropertiesByTable(tableName);
				Tag[] propertyTags = propertiesElement.getChild();
				for (Tag t : propertyTags) {
					PropertyElement p = (PropertyElement) t;
					ColumnType ct = new ColumnType(p.getType(), p.getFormat(), p.getColumn());
					cols.add(ct);
				}
				writer.setColumns(cols);
				writers.put(tableName, writer);

				writer.writeHead(def.getCfgElement().getSign());
			} catch (Exception e) {
				logger.error("写入表头时异常，原因：" + e.getMessage());
				e.printStackTrace();
				return false;
			}
			// 自动创建表
			PropertiesElement propertiesElement = def.getPropertiesByTable(tableName);
			if (propertiesElement != null && propertiesElement.isAutoCreateTable()) {
				try {
					createTable(propertiesElement);
				} catch (Exception e) {
					logger.error("创建表时出现异常，可能此表已存在，尝试直接在此表插入数据。表名：" + propertiesElement.getTable() + "   cause:" + e.getMessage());
				}
			}
		}
		return true;
	}

	private void addColumnTypeList(String scopeName, String ownerName, List<ColumnType> cols) {
		List<PropertyElement> globalCols = scopeMap.get(scopeName);
		if (globalCols != null) {
			for (PropertyElement e : globalCols) {
				if (scopeName.equals("global") && e.getTableName().equals(ownerName))
					continue;

				ColumnType ct = new ColumnType(e.getType(), e.getFormat(), e.getColumn());
				if (!cols.contains(ct)) {
					cols.add(ct);
				}
			}
		}
	}

	/**
	 * 在数据库中创建表
	 * 
	 * @param propertiesElement
	 *            表信息
	 * @throws Exception
	 */
	private void createTable(PropertiesElement propertiesElement) throws Exception {
		StringBuilder sb = new StringBuilder();
		sb.append("create table ").append(propertiesElement.getTable()).append(" (");
		if (propertiesElement.hasChild()) {
			Tag[] childs = propertiesElement.getChild();
			for (int i = 0; i < childs.length; i++) {
				PropertyElement property = (PropertyElement) childs[i];
				sb.append(property.getColumn()).append(" ");
				if (Util.isNotNull(property.getType()) && property.getType().equals("date")) {
					// 如果此列是date类型的话
					sb.append("date");
				} else {
					// 不是date类型，统一使用varchar2(1000)
					sb.append("varchar2(1000)");
				}
				if (i != childs.length - 1) {
					sb.append(",");
				}
			}
			sb.append(" )");
		}
		String sql = sb.toString();
		logger.debug("创建表语句：\n" + sql);
		try {
			CommonDB.executeUpdate(sql);
			logger.info("创建表成功,表名：" + propertiesElement.getTable());
		} catch (Exception e) {
			if (e instanceof SQLException) {
				SQLException sqle = (SQLException) e;
				logger.error("oracle错误码：" + sqle.getErrorCode());
			}
			throw e;
		}
	}

	/**
	 * 将外界变量转成实际的值。
	 * 
	 * @param value
	 *            外界变量名
	 * @return 实际值，如果没找到相应的变量，返回null
	 */
	protected String getSystemVar(String value) {
		String result = null;
		if (value.trim().equalsIgnoreCase("$OMC_ID$")) {
			result = String.valueOf(collectObjInfo.getDevInfo().getOmcID());
		} else if (value.trim().equalsIgnoreCase("$CITY_ID$")) {
			result = String.valueOf(collectObjInfo.getDevInfo().getCityID());
		} else if (value.trim().equalsIgnoreCase("$GATHER_TIME$")) {
			result = Util.getDateString(new Date());
		} else if (value.trim().equalsIgnoreCase("$DATA_TIME$")) {
			result = Util.getDateString(new Date(collectObjInfo.getLastCollectTime().getTime()));
		}

		return result;
	}

	public void dispose() {

	}

	// public CollectObjInfo getGatherObjInfo()
	// {
	// return gatherObjInfo;
	// }
	//
	// public void setGatherObjInfo(CollectObjInfo gatherObjInfo)
	// {
	// this.gatherObjInfo = gatherObjInfo;
	// }

	public GenericParser(CollectObjInfo TaskInfo) {
		super(TaskInfo);
		this.source = collectObjInfo.getCollectPath();
		GenericTemplet gt = (GenericTemplet) collectObjInfo.getParseTemplet();
		try {
			this.def = new Definition(gt.getTempLocation());
			scopeMap = def.getScopeMap();
			def.load();
		} catch (Exception e) {
			logger.error("加载模板时异常", e);
		}
	}

	private void init() {
		this.source = collectObjInfo.getCollectPath();
		GenericTemplet gt = (GenericTemplet) collectObjInfo.getParseTemplet();
		try {
			this.def = new Definition(SystemConfig.getInstance().getTempletPath() + File.separator + gt.getTempLocation());
			scopeMap = def.getScopeMap();
			def.load();
		} catch (Exception e) {
			logger.error("加载模板时异常", e);
		}
	}

	@Override
	public boolean parseData() {
		init();
		try {
			parse();
		} catch (Exception e) {
			logger.error("解析数据时发生异常", e);
			return false;
		}
		return true;
	}

	public String replceStr(String str) {
		StringBuffer sb = new StringBuffer();
		for (char s : str.toCharArray()) {
			if (s == '\r' || s == '\n') {
				continue;
			} else {
				sb.append(s);
			}
		}
		return sb.toString();
	}

}
