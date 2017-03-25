package db.dao;

import java.io.IOException;
import java.io.Writer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import oracle.sql.CLOB;
import util.CommonDB;
import util.DbPool;
import util.Util;
import db.pojo.RTask;
import framework.ConstDef;

public class RTaskDAO extends AbstractDAO<RTask> {

	@Override
	public int add(RTask entity) {
		int id = 0;
		if (entity == null) {
			return id;
		}
		String sql = "insert into igp_conf_rtask(id,taskid,filepath,collecttime,stamptime,collector_name,collectstatus,readopttype,cause) values(seq_igp_conf_rtask.nextval,%s,empty_clob(),to_date('%s','YYYY-MM-DD HH24:MI:SS'),sysdate,'%s',%s,%s,empty_clob())";
		sql = String.format(sql, entity.getTaskID(), entity.getCollectTime(), entity.getCollectorName(), entity.getCollectStatus(),
				entity.getReadoptType());
		Connection conn = DbPool.getConn();
		PreparedStatement st = null;
		ResultSet rs = null;
		try {
			conn.setAutoCommit(false);
			st = conn.prepareStatement(sql);
			if (st.executeUpdate() > 0) {
				rs = st.executeQuery("select seq_igp_conf_rtask.currval as id from dual");
				if (rs.next()) {
					id = rs.getInt("id");
					String filePath = entity.getFilePath();
					String cause = entity.getCause();
					if (filePath != null && !filePath.equals("")) {
						writeClob(rs, st, conn, "filepath", id, filePath);
					}
					if (cause != null && !cause.equals("")) {
						writeClob(rs, st, conn, "cause", id, cause);
					}

				}
			}
			conn.commit();
		} catch (SQLException e) {
			logger.error("执新增记录时异常：" + sql, e);
		} catch (IOException e) {
			logger.error("新增补采表路径时异常！", e);
		} finally {
			CommonDB.close(rs, st, conn);
		}
		return id;
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
	public PageQueryResult<RTask> pageQuery(int pageSize, int currentPage, String sql) {
		List<RTask> list = query(sql);
		int recordCount = list.size();
		int pageCount = getPageCount(pageSize, recordCount);
		RTaskPageQueryResult<RTask> result = null;

		int min = pageSize * (currentPage - 1);
		int max = pageSize * currentPage;
		List<RTask> reLi = new ArrayList<RTask>();
		for (int i = min; i < max; i++) {
			if (i >= recordCount)
				break;
			reLi.add(list.get(i));
		}
		result = new RTaskPageQueryResult<RTask>(pageSize, currentPage, pageCount, recordCount, reLi);
		return result;
	}

	/**
	 * 分页查询
	 * 
	 * @param pageSize
	 * @param currentPage
	 * @param conditions
	 *            Map<列名, 列值>
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public PageQueryResult<RTask> pageQuery(int pageSize, int currentPage, Map<String, String> conditions) {
		int recordCount = getRecordCount(conditions);
		int pageCount = getPageCount(pageSize, recordCount);
		RTaskPageQueryResult<RTask> result = null;

		int min = pageSize * (currentPage - 1);
		int max = pageSize * currentPage;
		StringBuilder sql = new StringBuilder(
				"SELECT B.* FROM (SELECT A.*, ROWNUM as num FROM (SELECT r.*,t.redo_time_offset FROM igp_conf_rtask r,igp_conf_task t where r.taskid=t.task_id ");
		sql = setConditions(sql, conditions);
		String executeSql = sql.toString() + ") A WHERE ROWNUM <= %s) B WHERE num >%s";
		executeSql = String.format(executeSql, max, min);
		List<RTask> list = query(executeSql);
		result = new RTaskPageQueryResult<RTask>(pageSize, currentPage, pageCount, recordCount, list);
		return result;
	}

	/**
	 * 条件设置
	 * 
	 * @param executeSql
	 * @param conditions
	 * @return
	 */
	private StringBuilder setConditions(StringBuilder executeSql, Map<String, String> conditions) {
		StringBuilder sql = executeSql;
		if (conditions != null && !conditions.isEmpty()) {
			StringBuilder shortSql = new StringBuilder();

			Set<String> columns = conditions.keySet();
			for (String column : columns) {
				shortSql.append(" and ");
				String value = conditions.get(column);
				if (column.equals("collector_name")) {
					shortSql.append("r." + column + " = '");
					shortSql.append(value + "'");
				} else if (column.equals("collectTime")) {
					shortSql.append("r." + column + " = to_date('");
					shortSql.append(value + "','YYYY-MM-DD HH24:MI:SS')");
				} else if (column.equals("collect_period")) {
					shortSql.append("t." + column + " = ");
					shortSql.append(value);
				} else if (column.equals("collect_type")) {
					shortSql.append("t." + column + " = ");
					shortSql.append(value);
				} else {
					shortSql.append("r." + column + " = ");
					shortSql.append(value);
				}
			}
			sql.append(shortSql);

		}
		return sql;
	}

	private int getPageCount(int pageSize, int recordCount) {
		int pageCount = 0;
		pageCount = recordCount / pageSize + (recordCount % pageSize == 0 ? 0 : 1);
		// 如果没有数据，那么就第一页
		pageCount = pageCount == 0 ? 1 : pageCount;
		return pageCount;
	}

	private int getRecordCount(Map<String, String> conditions) {
		int recordCount = 0;
		StringBuilder executeSql = new StringBuilder("select count(*) from igp_conf_rtask r,igp_conf_task t where r.taskid=t.task_id ");
		executeSql = setConditions(executeSql, conditions);
		Connection conn = DbPool.getConn();
		PreparedStatement st = null;
		ResultSet rs = null;
		try {
			st = conn.prepareStatement(executeSql.toString());
			rs = st.executeQuery();
			if (rs.next()) {
				recordCount = rs.getInt(1);
			}
		} catch (SQLException e) {
			logger.error("查询页时错误！" + executeSql.toString(), e);
		} finally {
			CommonDB.close(rs, st, conn);
		}
		return recordCount;

	}

	@Override
	public List<RTask> query(String sql) {
		List<RTask> rTasks = new ArrayList<RTask>();
		Connection con = DbPool.getConn();
		Statement st = null;
		ResultSet rs = null;
		try {
			st = con.createStatement();
			rs = st.executeQuery(sql);
			while (rs.next()) {
				rTasks.add(toRTask(rs));
			}
		} catch (Exception e) {
			logger.error("查询记录时异常：" + sql, e);
		} finally {
			CommonDB.close(rs, st, con);
		}
		return rTasks;

	}

	@Override
	public boolean delete(long id) {
		boolean flag = false;
		String sql = "delete from igp_conf_rtask r where r.id=" + id;
		try {
			flag = CommonDB.executeUpdate(sql) > 0;
		} catch (SQLException e) {
			logger.error("执行删除记录时异常：" + sql, e);
			flag = false;
		}
		return flag;
	}

	public int delMore(String[] idArray) {
		int result = 0;
		String executeSql = null;
		StringBuilder sql = new StringBuilder("delete from igp_conf_rtask r where");
		if (idArray != null && idArray.length != 0) {
			sql.append(" r.id in(");
			int len = idArray.length - 1;
			for (int i = 0; i < len; i++) {
				sql.append(idArray[i]).append(",");
			}
			sql.append(idArray[len]).append(")");
		}
		try {
			executeSql = sql.toString();
			result = CommonDB.executeUpdate(executeSql);
		} catch (SQLException e) {
			logger.error("执行批量删除记录时异常：" + executeSql, e);
		}
		return result;
	}

	@Override
	public boolean delete(RTask entity) {
		boolean flag = false;
		if (entity != null) {
			flag = delete(entity.getId());
		}
		return flag;
	}

	@Override
	public RTask getById(long id) {
		RTask r = null;
		String sql = "select r.* from igp_conf_rtask r,igp_conf_task t where r.taskid=t.task_id and r.id=" + id;
		List<RTask> list = query(sql);
		if (list != null && !list.isEmpty()) {
			r = list.get(0);
		}
		return r;
	}

	@Override
	public List<RTask> list() {
		String sql = "select r.*,t.redo_time_offset from igp_conf_rtask r,igp_conf_task t where r.taskid=t.task_id";
		return query(sql);
	}

	@Override
	public boolean update(RTask entity) {
		boolean flag = false;
		if (entity == null) {
			return flag;
		}
		String sql = "update igp_conf_rtask r set r.taskId=%s,r.filepath=empty_clob(),r.collecttime=to_date('%s','YYYY-MM-DD HH24:MI:SS'),r.stamptime=to_date('%s','YYYY-MM-DD HH24:MI:SS'),r.collector_name='%s',r.readopttype=%s,r.collectdegress=%s,r.collectstatus=%s where r.id=%s";
		sql = String.format(sql, entity.getTaskID(), entity.getCollectTime(), entity.getStampTime(), entity.getCollectorName(),
				entity.getReadoptType(), entity.getCollectDegress(), entity.getCollectStatus(), entity.getId());
		Connection conn = DbPool.getConn();
		PreparedStatement st = null;
		ResultSet rs = null;
		try {
			conn.setAutoCommit(false);
			st = conn.prepareStatement(sql);
			flag = st.executeUpdate() > 0;
			String filePath = entity.getFilePath();
			if (filePath.equals("")) {
				conn.commit();
				return flag;
			}
			writeClob(rs, st, conn, "filepath", entity.getId(), entity.getFilePath());
			conn.commit();
		} catch (Exception e) {
			logger.error("执行更新记录时异常：" + sql, e);
			flag = false;
		} finally {
			CommonDB.close(rs, st, conn);
		}

		return flag;
	}

	private void writeClob(ResultSet rs, Statement st, Connection conn, String column, long id, String value) throws SQLException, IOException {
		String selectsql = "select r." + column + " from igp_conf_rtask r where r.id=" + id + " for update";
		rs = st.executeQuery(selectsql);
		if (rs.next()) {
			CLOB clob = (CLOB) rs.getClob(column);
			Writer out = clob.getCharacterOutputStream();
			out.write(value);
			out.flush();
			out.close();
		}
	}

	private RTask toRTask(ResultSet rs) throws SQLException {
		RTask r = new RTask();
		r.setId(rs.getInt("id"));
		r.setTaskID(rs.getInt("taskid"));
		r.setFilePath(ConstDef.ClobParse(rs.getClob("filepath")));
		r.setCollectTime(Util.getDateString(rs.getTimestamp("collecttime")));
		Timestamp stamptime = rs.getTimestamp("stamptime");
		r.setStampTime(Util.getDateString(stamptime));
		r.setCollectorName(rs.getString("collector_name"));
		r.setReadoptType(rs.getInt("readopttype"));
		r.setCollectDegress(rs.getInt("collectdegress"));
		r.setCollectStatus(rs.getInt("collectstatus"));
		try {
			int redoTimeOffset = rs.getInt("REDO_TIME_OFFSET");
			long sTime = stamptime.getTime() + redoTimeOffset * 60 * 1000;
			String preStartTime = Util.getDateString(new Date(sTime));
			r.setPreStartTime(preStartTime);
		} catch (Exception e) {
		}
		r.setCause(ConstDef.ClobParse(rs.getClob("cause")));

		return r;
	}

	public static void main(String[] args) {
		// RTaskDAO r = new RTaskDAO();
		// int page = r.getPageCount(10);
		// System.out.println(page);
		Map<String, String> conditions = new HashMap<String, String>();
		conditions.put("aa", "bb");
		conditions.put("cc", "dd");
		conditions.put("ee", "ff");
		if (conditions != null && !conditions.isEmpty()) {
			Collection<String> obj = conditions.values();
			for (String string : obj) {
				System.out.println(string);
			}

		}
	}

}
