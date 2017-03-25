package parser.eric.pm;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;

import util.CommonDB;
import util.DBLogger;
import util.LogMgr;
import util.Util;

/**
 * 批量插入数据工具
 * 
 * @author liuwx 2010-6-18
 */
public class BatchInsertTool {

	private SqlldrParam param;

	private static DBLogger dbLogger = LogMgr.getInstance().getDBLogger();

	private static Logger log = LogMgr.getInstance().getSystemLogger();

	public BatchInsertTool(SqlldrParam param) {
		super();
		this.param = param;
	}

	/**
	 * 执行批量插入
	 * 
	 * @return
	 */
	public boolean execute() {
		if (param == null)
			return false;

		List<String> recordLst = new ArrayList<String>();
		Collection<ArrayList<String>> collection = param.records.values();
		log.debug("记录大小:" + collection.size());
		for (ArrayList<String> rList : collection) {
			String s = list2InsertSQL(rList);
			// log.debug(s);
			recordLst.add(s);
		}

		int succCount = recordLst.size();
		try {
			CommonDB.executeBatch(recordLst);
		} catch (SQLException e) {
			log.error("批量提交数据失败,原因:", e);
			succCount = 0;
		} finally {
			if (recordLst != null)
				recordLst.clear();
		}

		log.debug(param.taskID + ": omcid=" + param.omcID + " 表名=" + param.tbName + " 数据时间=" + param.dataTime + " 入库成功条数=" + succCount);

		dbLogger.log(param.omcID, param.tbName, param.dataTime, succCount, param.taskID);

		return true;
	}

	private String list2InsertSQL(List<String> lst) {
		StringBuilder sql = new StringBuilder();
		sql.append("insert into ").append(param.tbName).append(" (" + param.listColumn(",") + ",OMCID,COLLECTTIME,STAMPTIME) values(");

		String nowStr = Util.getDateString(new Date());
		String sysFieldValue = param.omcID + ",to_date('" + nowStr + "','YYYY-MM-DD HH24:MI:SS'),to_date('" + Util.getDateString(param.dataTime)
				+ "','YYYY-MM-DD HH24:MI:SS')";
		String commonFieldValue = param.fieldValue2String_1(param.commonFields, ","); // 获取公共字段值列表

		for (int i = 0; i < lst.size(); i++) {
			String value = lst.get(i);
			value = value == null ? "" : value; // 如果为null则为空
			sql.append("'" + value + "',");
		}
		sql.append(commonFieldValue).append(",").append(sysFieldValue);
		sql.append(")");

		return sql.toString();
	}
}
