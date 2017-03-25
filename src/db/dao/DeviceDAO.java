package db.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import util.CommonDB;
import util.DbPool;
import util.LogMgr;
import db.pojo.Device;

/**
 * 表igp_conf_device操作类
 * 
 * @author yuanxf
 * @since 1.0
 * @see Device
 */
public class DeviceDAO extends AbstractDAO<Device> {

	private Logger logger = LogMgr.getInstance().getSystemLogger();

	public int add(Device entity) {
		String sql = "insert into igp_conf_device values(?,?,?,?,?,?,?,?,?)";
		Connection con = DbPool.getConn();
		Statement st = null;
		PreparedStatement ps = null;
		try {
			con.setAutoCommit(false);
			ps = con.prepareStatement(sql);
			int index = 1;
			ps.setInt(index++, entity.getDevID());
			ps.setString(index++, entity.getDevName());
			ps.setInt(index++, entity.getCityID());
			ps.setInt(index++, entity.getOmcID());
			ps.setString(index++, entity.getVendor());
			ps.setString(index++, entity.getHostIP());
			ps.setString(index++, entity.getHostUser());
			ps.setString(index++, entity.getHostPwd());
			ps.setString(index++, entity.getHostSign());
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
	public boolean delete(Device entity) {
		return delete(entity.getDevID());
	}

	@Override
	public boolean delete(long id) {
		String sql = "delete igp_conf_device t where t.dev_id=" + id;
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
	public Device getById(long id) {
		Device dev = null;

		String sql = "select t.* from igp_conf_device t where t.dev_id=" + id;
		Connection con = DbPool.getConn();
		Statement st = null;
		ResultSet rs = null;
		try {
			st = con.createStatement();
			rs = st.executeQuery(sql);
			if (rs.next()) {
				dev = new Device();
				dev.setDevID(rs.getInt("DEV_ID"));
				dev.setDevName(rs.getString("DEV_NAME"));
				dev.setCityID(rs.getInt("CITY_ID"));
				dev.setOmcID(rs.getInt("OMCID"));
				dev.setVendor(rs.getString("VENDOR"));
				dev.setHostIP(rs.getString("HOST_IP"));
				dev.setHostUser(rs.getString("HOST_USER"));
				dev.setHostPwd(rs.getString("HOST_PWD"));
				dev.setHostSign(rs.getString("HOST_SIGN"));
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

		return dev;
	}

	public Device getByName(String name) {
		Device dev = null;

		String sql = "select t.* from igp_conf_device t where t.dev_name=" + name;
		Connection con = DbPool.getConn();
		Statement st = null;
		ResultSet rs = null;
		try {
			st = con.createStatement();
			rs = st.executeQuery(sql);
			if (rs.next()) {
				dev = new Device();
				dev.setDevID(rs.getInt("DEV_ID"));
				dev.setDevName(rs.getString("DEV_NAME"));
				dev.setCityID(rs.getInt("CITY_ID"));
				dev.setOmcID(rs.getInt("OMCID"));
				dev.setVendor(rs.getString("VENDOR"));
				dev.setHostIP(rs.getString("HOST_IP"));
				dev.setHostUser(rs.getString("HOST_USER"));
				dev.setHostPwd(rs.getString("HOST_PWD"));
				dev.setHostSign(rs.getString("HOST_SIGN"));
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

		return dev;
	}

	@Override
	public List<Device> list() {
		Device dev = null;

		String sql = "select * from igp_conf_device";
		List<Device> devs = new ArrayList<Device>();
		Connection con = DbPool.getConn();
		Statement st = null;
		ResultSet rs = null;
		try {
			st = con.createStatement();
			rs = st.executeQuery(sql);
			while (rs.next()) {
				dev = new Device();
				dev.setDevID(rs.getInt("DEV_ID"));
				dev.setDevName(rs.getString("DEV_NAME"));
				dev.setCityID(rs.getInt("CITY_ID"));
				dev.setOmcID(rs.getInt("OMCID"));
				dev.setVendor(rs.getString("VENDOR"));
				dev.setHostIP(rs.getString("HOST_IP"));
				dev.setHostUser(rs.getString("HOST_USER"));
				dev.setHostPwd(rs.getString("HOST_PWD"));
				dev.setHostSign(rs.getString("HOST_SIGN"));
				devs.add(dev);
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

		return devs;
	}

	/**
	 * 条件查询
	 * 
	 * @param dev
	 *            如果为NULL，则查询所有的记录
	 * @return
	 */
	public List<Device> criteriaQuery(Device dev) {
		int id = 0;
		String name = null;
		int omcID = 0;

		if (dev != null) {
			id = dev.getDevID();
			name = dev.getDevName();
			omcID = dev.getOmcID();
		}

		String basicSQL = "select t.* from igp_conf_device t";
		StringBuffer sql = new StringBuffer(basicSQL);

		// 存放所有的条件
		List<String> conditions = new ArrayList<String>();
		if (id > 0)
			conditions.add("t.dev_id=" + id);
		if (name != null)
			conditions.add("t.dev_name like '%" + name + "%'");
		if (omcID > 0)
			conditions.add("t.omcid=" + omcID);

		// 如果有条件
		if (conditions.size() >= 1) {
			sql.append(" where ").append(conditions.get(0));
			for (int i = 1; i < conditions.size(); i++) {
				sql.append(" and ").append(conditions.get(i));
			}
		}

		List<Device> devs = new ArrayList<Device>();
		Connection con = DbPool.getConn();
		Statement st = null;
		ResultSet rs = null;
		try {
			st = con.createStatement();
			rs = st.executeQuery(sql.toString());
			while (rs.next()) {
				dev = new Device();
				dev.setDevID(rs.getInt("DEV_ID"));
				dev.setDevName(rs.getString("DEV_NAME"));
				dev.setCityID(rs.getInt("CITY_ID"));
				dev.setOmcID(rs.getInt("OMCID"));
				dev.setVendor(rs.getString("VENDOR"));
				dev.setHostIP(rs.getString("HOST_IP"));
				dev.setHostUser(rs.getString("HOST_USER"));
				dev.setHostPwd(rs.getString("HOST_PWD"));
				dev.setHostSign(rs.getString("HOST_SIGN"));
				devs.add(dev);
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

		return devs;
	}

	@Override
	public boolean update(Device entity) {
		String sql = "update igp_conf_device set DEV_ID=?,DEV_NAME=?,CITY_ID=?,OMCID=?,VENDOR=?,HOST_IP=?,HOST_USER=?,HOST_PWD=?,HOST_SIGN=? where DEV_ID='"
				+ entity.getDevID() + "'";
		Connection con = DbPool.getConn();
		Statement st = null;
		PreparedStatement ps = null;
		try {
			con.setAutoCommit(false);
			ps = con.prepareStatement(sql);
			int index = 1;
			ps.setInt(index++, entity.getDevID());
			ps.setString(index++, entity.getDevName());
			ps.setInt(index++, entity.getCityID());
			ps.setInt(index++, entity.getOmcID());
			ps.setString(index++, entity.getVendor());
			ps.setString(index++, entity.getHostIP());
			ps.setString(index++, entity.getHostUser());
			ps.setString(index++, entity.getHostPwd());
			ps.setString(index++, entity.getHostSign());
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
	public boolean exists(Device entity) {
		if (entity == null)
			return false;

		// 如果id存在则认为是存在
		int devID = entity.getDevID();
		Device dev = this.getById(devID);
		if (dev != null) {
			return true;
		}

		boolean ret = false;

		// 如果城市编号,OMCID,ip都同时存在则认为存在
		int cityID = entity.getCityID();
		int omcID = entity.getOmcID();
		String ip = entity.getHostIP();

		String sql = "select t.* from igp_conf_device t where t.city_id=" + cityID + " and t.omcid=" + omcID + " and host_ip='" + ip + "'";
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

	public static void main(String[] args) {
		DeviceDAO dao = new DeviceDAO();
		List<Device> list = dao.list();
		for (Device de : list) {
			System.out.println(de.getDevID());
		}
	}

}
