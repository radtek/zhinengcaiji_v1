package db.dao;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Map;

import org.apache.log4j.Logger;

import parser.dt.Region;
import util.DatabaseUtil;
import util.DbPool;
import util.LogMgr;
import util.MapUtil;

/**
 * 场景和REGION信息查询DAO方法
 * 
 * @author chenrongqiang @ 2013-10-17
 */
public class RegionDAO {

	/**
	 * 单例对象类
	 */
	private static RegionDAO instance = new RegionDAO();

	/**
	 * 查询场景信息SQL语句
	 */
	private static final String QUERY_STATEMENT = "select id as piece_id,piecename,region_id,regionname from cfg_region_208";

	/**
	 * 日志打印
	 */
	protected static Logger LOGGER = LogMgr.getInstance().getSystemLogger();

	/**
	 * 私有的构造方法 防止外部通过其他方式实例化
	 */
	private RegionDAO() {
		super();
	}

	/**
	 * 工厂方法
	 * 
	 * @return
	 */
	public static RegionDAO getInstance() {
		return instance;
	}

	/**
	 * 场景信息查询DAO方法
	 * 
	 * @return Map<Long, Region> 查询到的REGION信息
	 */
	public Map<Long, Region> execute() {
		Connection con = DbPool.getConn();
		Map<Long, Region> datas = null;
		Statement st = null;
		ResultSet rs = null;
		try {
			st = con.createStatement();
			rs = st.executeQuery(QUERY_STATEMENT);
			ResultSetMetaData meta = rs.getMetaData();
			int number = meta.getColumnCount();
//			LOGGER.debug("本次查询到的场景和REGION信息共" + number + "条");
			// 如果没有查询到则返回空
			if (number <= 0)
				return null;
			// 如果查询到有数据 则根据实际的数据量初始化MAP的大小
			datas = MapUtil.create(number);
			while (rs.next()) {
				Region region = new Region();
				region.setPieceName(rs.getString("PIECENAME"));
				region.setRegionId(rs.getLong("REGION_ID"));
				region.setRegionName(rs.getString("REGIONNAME"));
				datas.put(rs.getLong("PIECE_ID"), region);
			}
			return datas;
		} catch (Exception e) {
			LOGGER.error("查询场景和REGION信息失败");
			return null;
		} finally {
			DatabaseUtil.close(con, st, rs);
		}
	}
}
