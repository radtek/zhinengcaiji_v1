package db.dao;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import util.CommonDB;
import util.DbPool;
import db.pojo.LogCltInsert;

/**
 * 表LOG_CLT_INSERT操作类
 * 
 * @author YangJian
 * @since 1.0
 */
public class LogCltInsertDAO extends AbstractDAO<LogCltInsert> {

	public LogCltInsertDAO() {
		super();
	}

	public int delete(byte calValue) {
		String sql = "delete LOG_CLT_INSERT t where t.IS_CAL=" + calValue;
		int i = 0;
		try {
			i = CommonDB.executeUpdate(sql);
		} catch (SQLException e) {
			logger.error("删除表LOG_CLT_INSERT中记录时异常:" + sql, e);
		}

		return i;
	}

	@Override
	public boolean delete(LogCltInsert entity) {
		return super.delete(entity);
	}

	@Override
	public List<LogCltInsert> list() {
		String sql = "select * from LOG_CLT_INSERT";
		List<LogCltInsert> lst = new ArrayList<LogCltInsert>();

		Connection con = DbPool.getConn();
		Statement st = null;
		ResultSet rs = null;

		try {
			st = con.createStatement();
			rs = st.executeQuery(sql);
			while (rs.next()) {
				LogCltInsert o = new LogCltInsert();
				o.setCalFlag(rs.getByte("IS_CAL"));
				o.setCount(rs.getInt("INSERT_COUNTNUM"));
				o.setOmcID(rs.getInt("OMCID"));
				o.setStampTime(new Date(rs.getTimestamp("STAMPTIME").getTime()));
				o.setTbName(rs.getString("CLT_TBNAME"));
				o.setVSysDate(new Date(rs.getTimestamp("VSYSDATE").getTime()));
				o.setTaskID(rs.getInt("TASKID"));

				lst.add(o);
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
		return lst;
	}

	// 单元测试
	public static void main(String[] args) {

	}
}
