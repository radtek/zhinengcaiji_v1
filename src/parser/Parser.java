package parser;

import java.sql.Timestamp;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.htmlparser.util.ParserException;

import task.CollectObjInfo;
import templet.Table.Column;
import util.HTMLTagCleaner;
import util.LogMgr;
import util.Util;
import distributor.Distribute;

/**
 * 解析器基类 特定解析类命名规则({网络标识(G|W|C)}{版本号}{数据格式(ASCII|BIN|扩展名)}[地市缩写]) Parser
 * 
 * @author IGP TDT
 * @since 1.0
 */
public abstract class Parser {

	// 采集参数配置信息，分析程序需要根据参配置进行分析
	protected CollectObjInfo collectObjInfo;

	protected Distribute distribute;

	// 系统数据时间,从此开始，后续从此开始所有的类都用此变量，因为不会受taskInfo.lastCollectTime更新而影响
	protected Timestamp dataTime; // 系统数据时间

	protected String fileName = "";// 需要解析原始文件的名称

	protected String dsConfigName = null; // 正在解析的文件(fileName成员属性)对应collect_path中的那个路径

	public String parentFileName = "";// 目录，用户省际边界协调单

	public Map<String, String> defaultValueColumns = null;

	public Set<String> htmlTagsFilterColumnsSet = null;

	protected Logger log = LogMgr.getInstance().getSystemLogger();

	public Parser(CollectObjInfo obj) {
		this.collectObjInfo = obj;
		distribute = new Distribute(obj);
	}

	public Parser() {
		super();
	}

	public String getFileName() {
		return this.fileName;
	}

	public void setFileName(String strFileName) {
		this.fileName = strFileName;
	}

	public String getDsConfigName() {
		return dsConfigName;
	}

	public void setDsConfigName(String dsConfigName) {
		this.dsConfigName = dsConfigName;
	}

	/**
	 * 使用无参构造函数时new出Parser后必须调用init方法初始化
	 * 
	 * @param gatherObjInfo
	 */
	public void init(CollectObjInfo obj) {
		this.collectObjInfo = obj;
		this.dataTime = new Timestamp(obj.getLastCollectTime().getTime());
		// this.distribute = new Distribute(obj);
	}

	public String defaultValueValidate(Column col, String colVal) {
		if (defaultValueColumns == null)
			return colVal;
		if (colVal != null && !"".equals(colVal))
			return colVal;
		if (defaultValueColumns.containsKey(col.getName().toUpperCase())) {
			colVal = defaultValueColumns.get(col.getName().toUpperCase());
		}
		return colVal;
	}

	public String htmlTagsFilter(Column col, String colVal) throws ParserException {
		if (htmlTagsFilterColumnsSet == null)
			return colVal;
		if (Util.isNull(colVal))
			return colVal;
		if (htmlTagsFilterColumnsSet.contains(col.getName().toUpperCase())) {
			HTMLTagCleaner cleaner = new HTMLTagCleaner();
			colVal = cleaner.cleanTag(colVal);
			cleaner = null;
		}
		return colVal;
	}

	// 数据分析接口，接收从采集模块传送过来的数据进行分析，
	public abstract boolean parseData() throws Exception;

	public CollectObjInfo getCollectObjInfo() {
		return collectObjInfo;
	}

	public void setCollectObjInfo(CollectObjInfo collectObjInfo) {
		this.collectObjInfo = collectObjInfo;
	}

	public Distribute getDistribute() {
		return distribute;
	}

	public void setDistribute(Distribute distribute) {
		this.distribute = distribute;
	}

	public Timestamp getDataTime() {
		return dataTime;
	}

	public void setDataTime(Timestamp dataTime) {
		this.dataTime = dataTime;
	}

	public String getParentFileName() {
		return parentFileName;
	}

	public void setParentFileName(String parentFileName) {
		this.parentFileName = parentFileName;
	}

}
