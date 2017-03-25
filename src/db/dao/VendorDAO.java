package db.dao;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import util.CommonDB;
import util.DbPool;
import db.pojo.Vendor;

/**
 * 表IGP_CONF_VENDOR操作类
 * 
 * @author YangJian
 * @since 1.0
 * @see Vendor
 */
public class VendorDAO extends AbstractDAO<Vendor> {

	public VendorDAO() {
		super();
	}

	@Override
	public boolean delete(long id) {
		String sql = "delete igp_conf_vendor v where v.id=" + id;
		int i = 0;
		try {
			i = CommonDB.executeUpdate(sql);
		} catch (SQLException e) {
			logger.error("删除记录时异常:" + sql, e);
		}

		return i > 0;
	}

	@Override
	public boolean delete(Vendor vendor) {
		return delete(vendor.getId());
	}

	@Override
	public List<Vendor> list() {
		String sql = "select * from igp_conf_vendor";
		List<Vendor> lst = new ArrayList<Vendor>();

		Connection con = DbPool.getConn();
		Statement st = null;
		ResultSet rs = null;

		try {
			st = con.createStatement();
			rs = st.executeQuery(sql);
			while (rs.next()) {
				Vendor v = new Vendor();
				v.setId(rs.getInt("ID"));
				v.setNameCH(rs.getString("VENDORNAME_CH"));
				v.setNameEN(rs.getString("VENDORNAME_EN"));

				lst.add(v);
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

	@Override
	public Vendor getById(long id) {
		Vendor v = null;

		String sql = "select t.* from igp_conf_vendor v where v.id=" + id;
		Connection con = DbPool.getConn();
		Statement st = null;
		ResultSet rs = null;
		try {
			st = con.createStatement();
			rs = st.executeQuery(sql);
			if (rs.next()) {
				v = new Vendor();
				v.setId(id);
				v.setNameCH(rs.getString("VENDORNAME_CH"));
				v.setNameEN(rs.getString("VENDORNAME_EN"));
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

		return v;
	}

	@Override
	public boolean update(Vendor vendor) {
		boolean bFlag = false;
		String sql = "update igp_conf_vendor set VENDORNAME_CH='%s',VENDORNAME_EN='%s' where id=%s";
		sql = String.format(sql, vendor.getNameCH(), vendor.getNameEN(), vendor.getId());
		sql = sql.replaceAll("=\'null\'", "=''");
		Connection con = DbPool.getConn();
		Statement st = null;
		ResultSet rs = null;
		try {
			st = con.createStatement();
			bFlag = st.executeUpdate(sql) >= 1;
		} catch (Exception e) {
			logger.error("修改记录时异常：" + sql, e);
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

		return bFlag;
	}

	@Override
	public int clearAll() {
		int count = 0;
		String sql = "delete igp_conf_vendor";
		try {
			count = CommonDB.executeUpdate(sql);
		} catch (SQLException e) {
			logger.error("删除记录时异常:" + sql, e);
		}
		return count;
	}

}
