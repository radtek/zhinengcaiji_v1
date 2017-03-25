package db.dao;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import util.CommonDB;
import util.Util;

import util.DbPool;
import db.pojo.CollectLog;

public class CollectLogDAO extends AbstractDAO<CollectLog> {

	@Override
	public List<CollectLog> list() {
		CollectLog log = null;

		String sql = "select * from igp_data_log";
		List<CollectLog> logs = new ArrayList<CollectLog>();
		Connection con = DbPool.getConn();
		Statement st = null;
		ResultSet rs = null;
		try {
			st = con.createStatement();
			rs = st.executeQuery(sql);
			while (rs.next()) {
				log = new CollectLog();
				log.setLogTime(Util.getDateString(rs.getTimestamp("LOG_TIME")));
				log.setTaskId(rs.getInt("TASK_ID"));
				log.setTaskDescription(rs.getString("TASK_DESCRIPTION"));
				log.setTaskType(rs.getString("TASK_TYPE"));
				log.setTaskStatus(rs.getString("TASK_STATUS"));
				log.setDataTime(Util.getDateString(rs.getTimestamp("DATA_TIME")));
				log.setCostTime(rs.getInt("COST_TIME"));
				log.setTaskResult(rs.getString("TASK_RESULT"));
				log.setTaskDetail(rs.getString("TASK_DETAIL"));
				log.setTaskException(rs.getString("TASK_EXCEPTION"));
				logs.add(log);
			}
		} catch (Exception e) {
			logger.error("查询记录时异常：" + sql, e);
		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
				if (st != null) {
					st.close();
				}
				if (con != null) {
					con.close();
				}
			} catch (Exception e) {
			}
		}

		return logs;
	}

	/**
	 * 分页查询
	 * 
	 * @param pageSize
	 * @param currentPage
	 * @param sql
	 *            要执行的sql
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public PageQueryResult<CollectLog> pageQuery(int pageSize, int currentPage, String sql) {
		List<CollectLog> list = query(sql);
		int recordCount = list.size();
		int pageCount = getPageCount(pageSize, recordCount);
		CollectLogPageQueryResult<CollectLog> result = null;

		int min = pageSize * (currentPage - 1);
		int max = pageSize * currentPage;
		List<CollectLog> reLi = new ArrayList<CollectLog>();
		for (int i = min; i < max; i++) {
			if (i >= recordCount)
				break;
			reLi.add(list.get(i));
		}
		result = new CollectLogPageQueryResult<CollectLog>(pageSize, currentPage, pageCount, recordCount, reLi);
		return result;
	}

	public int getPageCount(int pageSize, int recordCount) {
		int pageCount = 0;
		pageCount = recordCount / pageSize + (recordCount % pageSize == 0 ? 0 : 1);
		// 如果没有数据，那么就第一页
		pageCount = pageCount == 0 ? 1 : pageCount;
		return pageCount;
	}

	@Override
	public List<CollectLog> query(String sql) {
		List<CollectLog> logs = new ArrayList<CollectLog>();
		Connection con = DbPool.getConn();
		Statement st = null;
		ResultSet rs = null;
		try {
			st = con.createStatement();
			rs = st.executeQuery(sql);
			while (rs.next()) {
				logs.add(toCollectLog(rs));
			}
		} catch (Exception e) {
			logger.error("查询记录时异常：" + sql, e);
		} finally {
			CommonDB.close(rs, st, con);
		}
		return logs;

	}

	@SuppressWarnings("unchecked")
	public PageQueryResult<CollectLog> getTaskId(String taskId, int pageSize, int currentPage) {
		/*
		 * PageQueryResult<CollectLog> listLog = new PageQueryResult<CollectLog>(); String sql = "select * from igp_data_log where task_id="+taskId;
		 * List<CollectLog> logs = new ArrayList<CollectLog>(); Connection con = DbPool.getConn(); Statement st = null; ResultSet rs = null; try { st
		 * = con.createStatement(); rs = st.executeQuery(sql); while (rs.next()) { logs.add(toCollectLog(rs)); } listLog.getDatas().addAll(logs); }
		 * catch (Exception e) { logger.error("查询记录时异常：" + sql, e); } finally { CommonDB.close(rs, st, con); } return listLog;
		 */
		String sql = "select * from igp_data_log where task_id=" + taskId;
		List<CollectLog> list = query(sql);
		int recordCount = list.size();
		int pageCount = getPageCount(pageSize, recordCount);
		CollectLogPageQueryResult<CollectLog> result = null;

		int min = pageSize * (currentPage - 1);
		int max = pageSize * currentPage;
		List<CollectLog> reLi = new ArrayList<CollectLog>();
		for (int i = min; i < max; i++) {
			if (i >= recordCount)
				break;
			reLi.add(list.get(i));
		}
		result = new CollectLogPageQueryResult<CollectLog>(pageSize, currentPage, pageCount, recordCount, reLi);
		return result;

	}

	@SuppressWarnings("unchecked")
	public PageQueryResult<CollectLog> getTaskException(String taskId, int pageSize, int currentPage) {
		String sql = "select * from igp_data_log  where task_exception is not null";
		List<CollectLog> list = query(sql);
		int recordCount = list.size();
		int pageCount = getPageCount(pageSize, recordCount);
		CollectLogPageQueryResult<CollectLog> result = null;

		int min = pageSize * (currentPage - 1);
		int max = pageSize * currentPage;
		List<CollectLog> reLi = new ArrayList<CollectLog>();
		for (int i = min; i < max; i++) {
			if (i >= recordCount)
				break;
			reLi.add(list.get(i));
		}
		result = new CollectLogPageQueryResult<CollectLog>(pageSize, currentPage, pageCount, recordCount, reLi);
		return result;

	}

	@SuppressWarnings("unchecked")
	public PageQueryResult<CollectLog> selectLog(String sql, int pageSize, int currentPage) {
		List<CollectLog> list = query(sql);
		int recordCount = list.size();
		int pageCount = getPageCount(pageSize, recordCount);
		CollectLogPageQueryResult<CollectLog> result = null;

		int min = pageSize * (currentPage - 1);
		int max = pageSize * currentPage;
		List<CollectLog> reLi = new ArrayList<CollectLog>();
		for (int i = min; i < max; i++) {
			if (i >= recordCount)
				break;
			reLi.add(list.get(i));
		}
		result = new CollectLogPageQueryResult<CollectLog>(pageSize, currentPage, pageCount, recordCount, reLi);
		return result;

	}

	private CollectLog toCollectLog(ResultSet rs) throws SQLException {
		CollectLog log = new CollectLog();
		if (rs.getTimestamp("LOG_TIME") != null) {
			log.setLogTime(Util.getDateString(rs.getTimestamp("LOG_TIME")));
		}
		log.setTaskId(rs.getInt("TASK_ID"));
		log.setTaskDescription(rs.getString("TASK_DESCRIPTION"));
		log.setTaskType(rs.getString("TASK_TYPE"));
		log.setTaskStatus(rs.getString("TASK_STATUS"));
		if (rs.getTimestamp("DATA_TIME") != null) {
			log.setDataTime(Util.getDateString(rs.getTimestamp("DATA_TIME")));
		}
		log.setCostTime(rs.getInt("COST_TIME"));
		log.setTaskResult(rs.getString("TASK_RESULT"));
		log.setTaskDetail(rs.getString("TASK_DETAIL"));
		log.setTaskException(rs.getString("TASK_EXCEPTION"));
		return log;
	}

	public int pageCount(String sql) {
		List<CollectLog> list = query(sql);
		int recordCount = list.size();
		return recordCount;
	}

	public static void main(String[] args) {
		// CollectLogDAO log = new CollectLogDAO();
		// log.pageQuery(3,2,"select * from igp_data_log");
		// log.getTaskId("123");

	}

}
