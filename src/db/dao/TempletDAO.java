package db.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import util.CommonDB;
import util.DbPool;
import util.Util;
import db.pojo.Templet;

/**
 * 表igp_conf_templet操作类
 * 
 * @author yuanxf
 * @since 1.0
 * @see Templet
 */

public class TempletDAO extends AbstractDAO<Templet> {

	private static final String METADATASQL = "select column_name,data_type,data_length from user_tab_columns where upper(table_name)= ?";

	@Override
	public int add(Templet entity) {
		String sql = "insert into igp_conf_templet values(?,?,?,?,?)";
		Connection con = DbPool.getConn();
		Statement st = null;
		PreparedStatement ps = null;
		try {
			con.setAutoCommit(false);
			ps = con.prepareStatement(sql);
			int index = 1;
			ps.setInt(index++, entity.getTmpID());
			ps.setInt(index++, entity.getTmpType());
			ps.setString(index++, entity.getTmpName());
			ps.setString(index++, entity.getEdition());
			ps.setString(index++, entity.getTempFileName());

			ps.execute();
			if (con != null) {
				con.commit();
			}
			return 1;
		} catch (Exception e) {
			logger.error("插入数据失败：" + sql, e);
			try {
				if (con != null) {
					con.rollback();
				}
			} catch (Exception ex) {
			}
			return 0;
		} finally {
			try {
				if (st != null) {
					st.close();
				}
				if (con != null) {
					con.close();
				}
			} catch (Exception e) {
			}
		}
	}

	@Override
	public boolean delete(Templet entity) {
		return delete(entity.getTmpID());
	}

	@Override
	public boolean delete(long id) {
		String sql = "delete igp_conf_templet t where t.tmpid=" + id;
		int i = 0;
		try {
			i = CommonDB.executeUpdate(sql);
			return i > 0;
		} catch (SQLException e) {
			logger.error("删除记录时异常:" + sql, e);
			return false;
		}
	}

	@Override
	public Templet getById(long id) {
		Templet tem = null;

		String sql = "select t.* from igp_conf_templet t where t.tmpid=" + id;
		Connection con = DbPool.getConn();
		Statement st = null;
		ResultSet rs = null;
		try {
			st = con.createStatement();
			rs = st.executeQuery(sql);
			if (rs.next()) {
				tem = new Templet();
				tem.setTmpID(rs.getInt("TMPID"));
				tem.setTmpType(rs.getInt("TMPTYPE"));
				tem.setTmpName(rs.getString("TMPNAME"));
				tem.setEdition(rs.getString("EDITION"));
				tem.setTempFileName(rs.getString("TEMPFILENAME"));

			} else {
				return null;
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

		return tem;
	}

	@Override
	public List<Templet> list() {
		Templet tem = null;

		String sql = "select * from igp_conf_templet";
		List<Templet> tems = new ArrayList<Templet>();
		Connection con = DbPool.getConn();
		Statement st = null;
		ResultSet rs = null;
		try {
			st = con.createStatement();
			rs = st.executeQuery(sql);
			while (rs.next()) {
				tem = new Templet();
				tem.setTmpID(rs.getInt("TMPID"));
				tem.setTmpType(rs.getInt("TMPTYPE"));
				tem.setTmpName(rs.getString("TMPNAME"));
				tem.setEdition(rs.getString("EDITION"));
				tem.setTempFileName(rs.getString("TEMPFILENAME"));
				tems.add(tem);
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

		return tems;
	}

	@Override
	public boolean update(Templet entity) {
		String sql = "update igp_conf_templet set TMPID=?,TMPTYPE=?,TMPNAME=?,EDITION=?,TEMPFILENAME=? where tmpid='" + entity.getTmpID() + "'";
		Connection con = DbPool.getConn();
		Statement st = null;
		PreparedStatement ps = null;
		try {
			con.setAutoCommit(false);
			ps = con.prepareStatement(sql);
			int index = 1;
			ps.setInt(index++, entity.getTmpID());
			ps.setInt(index++, entity.getTmpType());
			ps.setString(index++, entity.getTmpName());
			ps.setString(index++, entity.getEdition());
			ps.setString(index++, entity.getTempFileName());
			int num = ps.executeUpdate();
			if (num < 1) {
				if (con != null) {
					con.rollback();
				}
				return false;
			}
			if (con != null) {
				con.commit();
			}
			return true;
		} catch (Exception e) {
			logger.error("更新数据失败：" + sql, e);
			e.printStackTrace();
			try {
				if (con != null) {
					con.rollback();
				}
			} catch (Exception ex) {
			}
			return false;
		} finally {
			try {
				if (st != null) {
					st.close();
				}
				if (con != null) {
					con.close();
				}
			} catch (Exception e) {
			}
		}
	}

	@Override
	public boolean exists(Templet entity) {
		if (entity == null)
			return false;

		// 如果id存在则认为是存在
		int tmpID = entity.getTmpID();
		Templet tmp = this.getById(tmpID);
		if (tmp != null) {
			return true;
		}

		boolean ret = false;

		// 如果模板文件名存在则认为存在
		String tmpFileName = entity.getTempFileName();
		// 如果为空，则认为不存在
		if (Util.isNull(tmpFileName))
			return false;

		String sql = "select t.* from igp_conf_templet t where t.tempfilename='" + tmpFileName + "'";
		Connection con = DbPool.getConn();
		Statement st = null;
		ResultSet rs = null;
		try {
			st = con.createStatement();
			rs = st.executeQuery(sql);
			if (rs.next()) {
				ret = true;
			}
		} catch (Exception e) {
			logger.error("查询记录时异常：" + sql, e);
			ret = true; // 这里采取保守的方法，出现异常时认为记录存在，上层调用这个方法异常的时候至少不会插入重复的数据
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

		return ret;
	}

	/**
	 * 条件查询
	 * 
	 * @param temp
	 *            如果为NULL，则查询所有的记录
	 */
	@Override
	public List<Templet> criteriaQuery(Templet temp) {
		int id = 0;
		String name = null;
		String des = null;
		int type = 0;

		if (temp != null) {
			id = temp.getTmpID();
			name = temp.getTempFileName();
			type = temp.getTmpType();
			des = temp.getTmpName();
		}

		String basicSQL = "select t.* from igp_conf_templet t";
		StringBuffer sql = new StringBuffer(basicSQL);

		// 存放所有的条件
		List<String> conditions = new ArrayList<String>();
		if (id > 0)
			conditions.add("t.tmpid=" + id);
		if (name != null)
			conditions.add("t.tempfilename like '%" + name + "%'");
		if (type > 0)
			conditions.add("t.tmptype=" + type);
		if (des != null)
			conditions.add("t.tmpname like '%" + des + "%'");

		// 如果有条件
		if (conditions.size() >= 1) {
			sql.append(" where ").append(conditions.get(0));
			for (int i = 1; i < conditions.size(); i++) {
				sql.append(" and ").append(conditions.get(i));
			}
		}

		List<Templet> tmps = new ArrayList<Templet>();
		Connection con = DbPool.getConn();
		Statement st = null;
		ResultSet rs = null;
		Templet tTmp = null;
		try {
			st = con.createStatement();
			rs = st.executeQuery(sql.toString());
			while (rs.next()) {
				tTmp = new Templet();
				tTmp.setTmpID(rs.getInt("TMPID"));
				tTmp.setTmpType(rs.getInt("TMPTYPE"));
				tTmp.setTmpName(rs.getString("TMPNAME"));
				tTmp.setEdition(rs.getString("EDITION"));
				tTmp.setTempFileName(rs.getString("TEMPFILENAME"));
				tmps.add(tTmp);
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

		return tmps;
	}

	/**
	 * 取得数据库中对应表的元数据， 参数表名
	 * 
	 * @param tableName
	 * @return
	 */
	public List<String> getMetaData(String tableName) {
		Connection connection = DbPool.getConn();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		List<String> list = new ArrayList<String>();
		try {
			stmt = connection.prepareStatement(METADATASQL);
			stmt.setString(1, tableName.toUpperCase());
			rs = stmt.executeQuery();
			while (rs.next()) {
				list.add(rs.getString(1));
			}
			rs.close();
			stmt.close();
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("获取元数据时出现异常.", e);
		} finally {
			try {
				if (stmt != null) {
					stmt.close();
				}
				if (connection != null) {
					connection.close();
				}
			} catch (Exception e) {
			}
		}
		return list;
	}

	// 单元测试
	public static void main(String[] args) {
		Templet tm = new Templet();
		tm.setTmpID(800);
		tm.setTmpType(22);
		tm.setTmpName("igp_1.0_模板");
		tm.setEdition("1.0");
		tm.setTempFileName("igpTemplet.xml");

		TempletDAO dao = new TempletDAO();

		// dao.update(tm);
		Templet t = dao.getById(50);
		System.out.println(t.getTempFileName());

		dao.delete(tm);
	}

}
