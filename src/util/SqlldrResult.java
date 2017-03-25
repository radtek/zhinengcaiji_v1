package util;

import java.util.List;

/**
 * 执行sqlldr命令的结果
 * 
 * @author ChenSijiang
 * @since 1.0
 */
public class SqlldrResult extends Result {

	private String maxFileSize;// 最大文件大小

	private boolean isOracleLog;// 是否是oracle日志文件

	private String tableName;// 表名

	private int loadSuccCount = 0;// 载入成功行数

	private int data;// 数据错误行数没有加载

	private int when;// when子句失败行数没有加载

	private int nullField;// 字段为空行数

	private int skip;// 跳过的逻辑记录总数

	private int read;// 读取的逻辑记录总数

	private int refuse;// 拒绝的逻辑记录总数

	private int abandon;// 废弃的逻辑记录总数

	private String startTime;// 开始运行时间

	private String endTime;// 运行结束时间

	private List<String> ruleList;

	public SqlldrResult() {
		this(0);
	}

	public SqlldrResult(int code, String message, Result cause) {
		super(code, message, cause);
	}

	public SqlldrResult(int code, String message) {
		super(code, message);
	}

	public SqlldrResult(int code) {
		super(code);
	}

	public SqlldrResult(Result cause) {
		super(cause);
	}

	public String getMaxFileSize() {
		return maxFileSize;
	}

	public void setMaxFileSize(String maxFileSize) {
		this.maxFileSize = maxFileSize;
	}

	public boolean isOracleLog() {
		return isOracleLog;
	}

	public void setOracleLog(boolean isOracleLog) {
		this.isOracleLog = isOracleLog;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public int getLoadSuccCount() {
		return loadSuccCount;
	}

	public void setLoadSuccCount(int loadSuccCount) {
		this.loadSuccCount = loadSuccCount;
	}

	public int getData() {
		return data;
	}

	public int getLoadFailCount() {
		return data;
	}

	public void setData(int data) {
		this.data = data;
	}

	public int getWhen() {
		return when;
	}

	public void setWhen(int when) {
		this.when = when;
	}

	public int getNullField() {
		return nullField;
	}

	public void setNullField(int nullField) {
		this.nullField = nullField;
	}

	public int getSkip() {
		return skip;
	}

	public void setSkip(int skip) {
		this.skip = skip;
	}

	public int getRead() {
		return read;
	}

	public void setRead(int read) {
		this.read = read;
	}

	public int getRefuse() {
		return refuse;
	}

	public void setRefuse(int refuse) {
		this.refuse = refuse;
	}

	public int getAbandon() {
		return abandon;
	}

	public void setAbandon(int abandon) {
		this.abandon = abandon;
	}

	public String getStartTime() {
		return startTime;
	}

	public void setStartTime(String startTime) {
		this.startTime = startTime;
	}

	public String getEndTime() {
		return endTime;
	}

	public void setEndTime(String endTime) {
		this.endTime = endTime;
	}

	public List<String> getRuleList() {
		return ruleList;
	}

	public void setRuleList(List<String> ruleList) {
		this.ruleList = ruleList;
	}
}
