package db.dao;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import util.DbPool;
import util.Util;
import db.pojo.LogCltInsert;

/**
 * 汇总接口表
 * 
 * @author chensj
 * @since 1.0
 */
public class SummaryDAO extends AbstractDAO<LogCltInsert> {

	@Override
	public int add(LogCltInsert entity) {
		return super.add(entity);
	}

	@Override
	public int clearAll() {
		return super.clearAll();
	}

	@Override
	public List<LogCltInsert> criteriaQuery(LogCltInsert dev) {
		return super.criteriaQuery(dev);
	}

	@Override
	public boolean delete(long id) {
		return super.delete(id);
	}

	@Override
	public boolean delete(LogCltInsert entity) {
		return super.delete(entity);
	}

	@Override
	public boolean exists(LogCltInsert entity) {
		return super.exists(entity);
	}

	@Override
	public LogCltInsert getById(long id) {
		return super.getById(id);
	}

	@Override
	public LogCltInsert getByName(String name) {
		return super.getByName(name);
	}

	@Override
	public List<LogCltInsert> list() {
		return super.list();
	}

	@Override
	public PageQueryResult<LogCltInsert> pageQuery(int pageSize, int currentPage) {
		return pageQuery(null, pageSize, currentPage);
	}

	public PageQueryResult<LogCltInsert> pageQuery(LogCltInsert condition, int pageSize, int currentPage) {
		int start = pageSize * currentPage - pageSize + 1; // 算出此页第一条记录的rownum
		int end = pageSize * currentPage; // 算出此页最后一条记录的rownum
		int recordCount = 0; // 查询出来的记录数
		int totalCount = 0;// 总行数
		String sql = "";
		String prefix = "with partdata as (";
		String tmp = null;
		if (condition == null) {
			sql = "with partdata as (select rownum rowno,t.* from log_clt_insert t )"
					+ " select * from partdata where rowno between __start and __end";
			tmp = "select count(*) c from log_clt_insert";
		} else {
			StringBuilder b = new StringBuilder();
			b.append("select rownum rowno,t.* from log_clt_insert t where 1<>0");
			if (condition.getOmcID() > -1) // omcid大于等于0的，当作有效
			{
				b.append(" and t.omcid=").append(condition.getOmcID());
			}
			if (condition.getCalFlag() > -1) {
				b.append(" and t.iscal=").append(condition.getCalFlag());
			}
			if (condition.getCount() > -1) {
				b.append(" and t.insert_countnum=").append(condition.getCount());
			}
			if (condition.getStampTime() != null) {
				b.append(" and t.stamptime=to_date('").append(Util.getDateString(condition.getStampTime())).append("','yyyy-mm-dd hh24:mi:ss')");
			}
			if (condition.getTbName() != null) {
				b.append(" and t.clt_tbname like '%").append(condition.getTbName()).append("%'");
			}
			if (condition.getVSysDate() != null) {
				b.append(" and t.vsysdate=to_date('").append(Util.getDateString(condition.getVSysDate())).append("','yyyy-mm-dd hh24:mi:ss')");
			}
			if (condition.getTaskID() > -1) {
				b.append(" and t.taskid=").append(condition.getTaskID());
			}
			tmp = b.toString();
			b.append(") select * from partdata where rowno between __start and __end");
			b.insert(0, prefix);
			sql = b.toString();
		}
		sql = sql.replace("__start", String.valueOf(start));
		sql = sql.replace("__end", String.valueOf(end));
		List<LogCltInsert> list = new ArrayList<LogCltInsert>();

		Connection con = DbPool.getConn();
		Statement st = null;
		ResultSet rs = null;
		try {
			con.setAutoCommit(false);

			st = con.createStatement();
			tmp = tmp.replace("rownum rowno,t.*", "count(*) c");
			rs = st.executeQuery(tmp);
			rs.next();
			totalCount = rs.getInt("c");
			rs = st.executeQuery(sql);
			while (rs.next()) {
				LogCltInsert lci = new LogCltInsert();
				lci.setCalFlag((byte) rs.getInt("is_cal"));
				lci.setCount(rs.getInt("insert_countnum"));
				lci.setOmcID(rs.getInt("omcid"));
				lci.setStampTime(rs.getTimestamp("stamptime"));
				lci.setTaskID(rs.getInt("taskid"));
				lci.setTbName(rs.getString("clt_tbname"));
				lci.setVSysDate(rs.getTimestamp("vsysdate"));
				list.add(lci);
				recordCount++;
			}
			con.commit();
		} catch (Exception e) {
			logger.error("查询log_clt_insert表时异常", e);
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
		int pageCount = totalCount % pageSize == 0 ? totalCount / pageSize : totalCount / pageSize + 1; // 算出当前pareSize情况下的总页数
		if (recordCount == 0) {
			pageCount = 1;
			currentPage = 1;
		}
		PageQueryResult<LogCltInsert> pageQueryResult = new PageQueryResult<LogCltInsert>(pageSize, currentPage, pageCount, list);
		return pageQueryResult;
	}

	@Override
	public List<LogCltInsert> query(String sql) {
		return super.query(sql);
	}

	@Override
	public boolean update(LogCltInsert entity) {
		return super.update(entity);
	}

	@Override
	public boolean validate(LogCltInsert entity) {
		return super.validate(entity);
	}

	public static void main(String[] args) {
		String s = "select rownum rowno,t.* from log_clt_insert t where 1<>0";
		System.out.println(s.replace("rownum rowno,t.*", "count(*) c"));
	}
}
