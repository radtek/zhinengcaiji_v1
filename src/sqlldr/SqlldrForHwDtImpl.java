package sqlldr;

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import util.DBLogger;
import util.ExternalCmd;
import util.LogMgr;
import util.SqlldrResult;
import util.Util;
import util.loganalyzer.SqlLdrLogAnalyzer;
import framework.SystemConfig;

/**
 * 针对路测数据实现的SQLLDR
 * 
 * @author JerryLi616 @ 2013年9月24日
 */
public class SqlldrForHwDtImpl{

	private Map<String,List<String>> tableCols;

	private long taskId;

	private int omcId;

	private Timestamp time_date;

	private String time_str;

	private String logKey;

	private SystemConfig cfg;

	private String flag;

	// 每个表每一行要预留的空间，用于重写数据
	private String spaceStr;

	public String getSpaceStr(){
		return spaceStr;
	}

	public void setSpaceStr(String spaceStr){
		this.spaceStr = spaceStr;
	}

	// SQLLDR信息
	private SqlldrInfoForHwDt info = null;

	public SqlldrInfoForHwDt getInfo(){
		return info;
	}

	public void setInfo(SqlldrInfoForHwDt info){
		this.info = info;
	}

	// sqlldr日志分析器
	private SqlLdrLogAnalyzer analyzer = null;

	// 系统日志
	private static final Logger logger = LogMgr.getInstance().getSystemLogger();

	// db日志
	private static final DBLogger dbLogger = LogMgr.getInstance().getDBLogger();

	// 需要转日期的字段
	private static List<String> dateFields = Arrays.asList("COLLECT_TIME", "START_TIME", "END_TIME", "TESTSTARTTIME");

	// 需要去空格再转Number的字段
	private static List<String> numberFields = Arrays.asList("NE_BSC_ID", "NE_BTS_ID", "NE_CELL_ID", "NE_CARR_ID",
			"BSC_ID", "BTS_ID", "CELL_ID", "CARR", "NI_NE_CELL_ID", "NI_NE_CARR_ID", "NI_NE_CARR_ID",
			"CHANNELTYPE", "TXPOWER", "PROBLEM_EVT_NUM");

	// 需要去空格的字段
	private static List<String> trimFields = Arrays.asList("CELL_NAME", "BSC_NAME", "NI_CELL_NAME", "HEXCONTENT", "URL");

	public SqlldrForHwDtImpl(long taskId, int omcId, Timestamp time, String desc){
		super();
		this.taskId = taskId;
		this.omcId = omcId;
		this.time_date = time;
		this.time_str = Util.getDateString(time);
		this.logKey = String.format("[%s][%s]", taskId, time_str);
		this.cfg = SystemConfig.getInstance();
		this.flag = desc;
	}

	public void setTableCols(Map<String,List<String>> tableCols){
		this.tableCols = tableCols;
	}

	public int initSqlldr(){
		StringBuffer sb = new StringBuffer();
		// TODO Auto-generated method stub
		File dir = new File(cfg.getCurrentPath() + File.separator + flag + File.separator + taskId + File.separator);
		if(!dir.exists()){
			dir.mkdirs();
		}
		long rnd = System.currentTimeMillis();
		Iterator<String> it = tableCols.keySet().iterator();
		try{
			while(it.hasNext()){
				String tableName = it.next();
				List<String> cols = tableCols.get(tableName);
				String baseName = taskId + "_" + tableName + "_" + Util.getDateString_yyyyMMddHHmmss(this.time_date)
						+ "_" + rnd;
				info = new SqlldrInfoForHwDt(new File(dir, baseName + ".txt"), new File(dir, baseName + ".log"),
						new File(dir, baseName + ".bad"), new File(dir, baseName + ".ctl"));
				info.writerForCtl.println("LOAD DATA");
				info.writerForCtl.println("CHARACTERSET " + cfg.getSqlldrCharset());
				info.writerForCtl.println("INFILE '" + info.txt.getAbsolutePath() + "' APPEND INTO TABLE " + tableName);
				info.writerForCtl.println("FIELDS TERMINATED BY \"|\"");
				info.writerForCtl.println("TRAILING NULLCOLS");
				info.writerForCtl.print("(");
				for(int i = 0; i < cols.size(); i++){
					String colName = cols.get(i);
					info.writerForCtl.print("\"" + colName + "\"");
					sb.append(colName);
					if(dateFields.contains(colName)){
						info.writerForCtl.print(" DATE 'YYYY-MM-DD HH24:MI:SS'");
					}
					if(trimFields.contains(colName)){
						info.writerForCtl.print(" char(1000) \"trim(:" + colName + ")\"");
					}
					if(numberFields.contains(colName)){
						info.writerForCtl.print(" \"to_number(trim(:" + colName + "))\"");
					}
					if(i < cols.size() - 1){
						info.writerForCtl.print(",");
						sb.append("|");
					}
				}
				info.writerForCtl.print(")");
				sb.append("\n");
				info.rafForTxt.write(sb.toString().getBytes());
				info.writerForCtl.flush();
				info.writerForCtl.close();
			}
		}catch(Exception e){
			logger.error("SqlldrForHwDtImpl.initSqlldr() - sqlldr时异常", e);
		}
		return sb.length();
	}

	public boolean runSqlldr(){
		// TODO Auto-generated method stub
		analyzer = new SqlLdrLogAnalyzer();
		String cmd = String.format("sqlldr userid=%s/%s@%s skip=%s control=%s bad=%s log=%s errors=9999999",
				cfg.getDbUserName(), cfg.getDbPassword(), cfg.getDbService(), 1, info.ctl.getAbsoluteFile(),
				info.bad.getAbsoluteFile(), info.log.getAbsoluteFile());
		int ret = -1;
		try{
			logger.debug(logKey + "执行sqlldr - " + cmd.replace(cfg.getDbPassword(), "*"));
			ret = new ExternalCmd().execute(cmd);
			SqlldrResult result = analyzer.analysis(info.log.getAbsolutePath());
			dbLogger.log(omcId, result.getTableName(), this.time_str, result.getLoadSuccCount(), taskId);
			logger.debug(logKey
					+ String.format("ret=%s,入库条数=%s,task_id=%s,表名=%s,时间点=%s", ret, result.getLoadSuccCount(), taskId,
							result.getTableName(), time_str));
			if(ret == 0 && cfg.isDeleteLog()){
				info.txt.delete();
				info.log.delete();
				info.ctl.delete();
				info.bad.delete();
			}
		}catch(Exception e){
			logger.error(logKey + " - sqlldr时异常", e);
			return false;
		}finally{
			try{
				info.rafForTxt.close();
			}catch(IOException e){
				logger.error(logKey + "SqlldrForHwDtImpl.runSqlldr() - info.rafForTxt.close()时异常", e);
			}
		}
		return true;
	}

	/**
	 * @param args
	 */
	public static void main(String [] args){
		// TODO Auto-generated method stub

	}

}
