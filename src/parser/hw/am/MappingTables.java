package parser.hw.am;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.apache.log4j.Logger;

import util.CommonDB;
import util.DbPool;
import util.LogMgr;
import util.Util;

/**
 * 
 * MappingTables
 * 
 * @author
 * @version 1.0<br>
 *          1.0.1 liangww 2012-04-23 修改为延时加载相关omcid<br>
 *          1.0.2 liangww 2012-05-25 增加NE_BSC_W_MAP，NE_BSC_W_MAP，NE_BTS_W_MAP两个索引map, 并把mapHw,ObjectInstance内部类移到外面 <br>
 *          1.0.3 liangww 2012-05-28 删除OBJ_INS静态变量与及其相应的代码。<br>
 *          1.0.4 liangww 2012-05-29 修改getHwMap时，网元名是获取MapHw对象相关级别的<br>
 *          1.0.5 liangww 2012-05-30 neCellId,neBtsId为空表示不是cell, bts级别，不放到内存索引中 1.0.2 liangww 2012-06-20 删除那些内存索引map，把相关的方法修改为直接查询数据库
 */
public final class MappingTables {

	private static final Logger log = LogMgr.getInstance().getSystemLogger();

	// liangww add 2012-05-25
	public final static int OMC_LEVEL = 1; // omc

	public final static int BSC_LEVEL = 2; // bsc

	public final static int BTS_LEVEL = 3; // bts

	public final static int CELL_LEVEL = 4; // cell

	// <query_clt_pm_w_hw_objectinstance>
	private static final String SQL_3G_HW_OBJECT_INSTANCE = "select omcid, nefdn, objecttypeid, objectno,"
			+ " nename, objectmember0 as objfdn from clt_pm_w_hw_objectinstance where stamptime = (select max(stamptime) from clt_pm_w_hw_objectinstance)"
			+ " and omcid =  ?   and objectmember0 = ?";

	// </query_clt_pm_w_hw_objectinstance>
	// <query_cfg_map_hw_ne_objectno>
	private static final String SQL_HW_NE_MAP = "select omcid, objecttypeid, objectno, city_id,"
			+ " ne_cell_id, ne_bts_id, version , cell_name, bts_name from cfg_map_hw_ne_objectno "
			+ " where omcid = ? and objecttypeid = ? and objectno = ?";

	// </query_cfg_map_hw_ne_objectno>

	//
	private static final String SQL_NE_BTS_W = "select t.ne_bts_id,  omcid, t.ne_bsc_id, t.bts_id, bts_name, city_id   "
			+ " from ne_bts_w t  where t.vendor='ZY0808' and t.omcid = ? and t.ne_bsc_id = ? and t.bts_id = ?";

	//
	private static final String SQL_NE_BSC_W = "select ne_bsc_id, bsc_name , omcid, version from ne_bsc_w t  "
			+ " where t.vendor='ZY0808' and t.omcid = ? and t.bsc_name = ?";

	// 2g
	// <query_clt_pm_w_hw_objectinstance>
	private static final String SQL_2G_HW_OBJECT_INSTANCE = "select omcid, nefdn, objecttypeid, objectno,"
			+ " nename, objectmember0 as objfdn from clt_pm_hw_v9r11_tblobjins where stamptime = (select max(stamptime) from clt_pm_hw_v9r11_tblobjins)"
			+ " and omcid =  ?   and objectmember0 = ?";

	private MappingTables() {
		super();
	}

	private static MapHW getHwMapFromTable(int omcId, long objectTypeId, long objectNo) {
		MapHW mapHW = null;
		double end = 0;
		PreparedStatement st = null;
		ResultSet rs = null;
		Connection con = null;
		String sql = SQL_HW_NE_MAP;
		double begin = System.currentTimeMillis();

		// 组装sql
		sql = SQL_HW_NE_MAP;
		// log.debug("开始查询cfg_map_hw_ne_objectno数据，SQL=" + sql);
		try {
			con = DbPool.getConn();
			st = con.prepareStatement(sql);
			st.setInt(1, omcId);
			st.setLong(2, objectTypeId);
			st.setLong(3, objectNo);

			rs = st.executeQuery();
			if (rs.next()) {
				String neCellId = rs.getString("ne_cell_id");
				String neBtsId = rs.getString("ne_bts_id");
				String version = rs.getString("version");
				// liangww add 2012-05-29 增加cellName, btsName
				String cellName = rs.getString("cell_name");
				String btsName = rs.getString("bts_name");

				// liangww modify 2012-5-29
				mapHW = new MapHW(neCellId, neBtsId, version);
				mapHW.btsName = btsName;
				mapHW.cellName = cellName;
				// liangww modify 2012-07-02 给mapHw.cityId赋值
				mapHW.cityId = rs.getInt("city_id");
			}
		} catch (Exception e) {
			log.warn("查询出的cfg_map_hw_ne_objectno异常，sql=" + sql, e);
		} finally {
			CommonDB.close(rs, st, con);
		}

		end = System.currentTimeMillis();
		log.debug(String.format("查询cfg_map_hw_ne_objectno，耗时=%s秒", (end - begin) / 1000.));

		return mapHW;
	}

	/**
	 * 
	 * @param omcIds
	 * @param bClean
	 * @return
	 */
	private static ObjectInstance getObjectInstanceFromTable(int omcId, String objFdn) {
		ObjectInstance objectInstance = null;
		Connection con = null;
		PreparedStatement st = null;
		ResultSet rs = null;
		double begin = System.currentTimeMillis();
		double end = 0;
		String sql = SQL_3G_HW_OBJECT_INSTANCE;

		// 组装sql objFdn
		sql = SQL_3G_HW_OBJECT_INSTANCE;
		try {
			con = DbPool.getConn();
			st = con.prepareStatement(sql);
			st.setInt(1, omcId);
			st.setString(2, objFdn);

			rs = st.executeQuery();
			if (rs.next()) {
				String neFdn = rs.getString("nefdn");
				long objectTypeId = rs.getLong("objecttypeid");
				long objectNo = rs.getLong("objectno");

				objectInstance = new ObjectInstance(neFdn, objectTypeId, objectNo);
			}
		} catch (Exception e) {
			log.error("载入clt_pm_w_hw_objectinstance数据时发生异常。", e);
		} finally {
			CommonDB.close(rs, st, con);
		}
		end = System.currentTimeMillis();
		log.debug(String.format("clt_pm_w_hw_objectinstance数据查询完毕，耗时=%s秒", (end - begin) / 1000.));

		return objectInstance;
	}

	/**
	 * 
	 * @param omcIds
	 * @param bClean
	 * @return
	 */
	private static NeBtsW getNeBtsWFromTable(int omcId, String neBscId, String btsId) {
		NeBtsW neBtsW = null;
		double end = 0;
		PreparedStatement st = null;
		ResultSet rs = null;
		Connection con = null;
		String sql = SQL_NE_BTS_W;
		double begin = System.currentTimeMillis();

		// 组装sql
		sql = SQL_NE_BTS_W;
		// log.debug("开始查询ne_bts_w数据，SQL=" + sql);

		try {
			con = DbPool.getConn();
			st = con.prepareStatement(sql);
			st.setInt(1, omcId);
			// ne_bsc_id = ? and t.bts_id = ?
			st.setString(2, neBscId);
			st.setString(3, btsId);

			rs = st.executeQuery();
			if (rs.next()) {
				int cityId = rs.getInt("city_id");
				String neBtsId = rs.getString("ne_bts_id");
				String btsName = rs.getString("bts_name");
				// neBscId = rs.getString("ne_bsc_id");
				// btsId = rs.getString("bts_id");
				neBtsW = new NeBtsW(omcId, cityId, neBtsId, btsName);
			}
		} catch (Exception e) {
			log.error("查询ne_bts_w数据时发生异常。", e);
		} finally {
			CommonDB.close(rs, st, con);
		}

		end = System.currentTimeMillis();
		log.debug(String.format("ne_bts_w数据查询完毕，耗时=%s秒", (end - begin) / 1000., sql));
		return neBtsW;
	}

	/**
	 * 
	 * @param omcIds
	 * @param bClean
	 * @return
	 */
	private static NeBscW getNeBscWFromTable(int omcId, String bscName) {
		NeBscW neBscW = null;
		double end = 0;
		PreparedStatement st = null;
		ResultSet rs = null;
		Connection con = null;
		String sql = SQL_NE_BSC_W;
		double begin = System.currentTimeMillis();

		// 组装sql
		sql = SQL_NE_BSC_W;
		// log.debug("开始查询ne_bsc_w数据，SQL=" + sql);

		try {
			con = DbPool.getConn();
			st = con.prepareStatement(sql);
			st.setInt(1, omcId);
			st.setString(2, bscName);
			rs = st.executeQuery();
			if (rs.next()) {
				String neBscId = rs.getString("ne_bsc_id");
				String version = rs.getString("version");
				neBscW = new NeBscW(omcId, neBscId, bscName, version);
			}
		} catch (Exception e) {
			log.error("查询ne_bsc_w数据时发生异常。", e);
		} finally {
			CommonDB.close(rs, st, con);
		}

		end = System.currentTimeMillis();
		log.debug(String.format("ne_bsc_w数据查询完毕，耗时=%s秒", (end - begin) / 1000., sql));
		return neBscW;
	}

	public static QueriedEntry findHw3G(int omcId, int neLevel, String objFdn) {
		ObjectInstance oi = null;
		if (Util.isNull(objFdn)) {
			return null;
		}
		// 如果OBJFDN 不为空 通过 OMCID,OBJFDN 关联 CLT_PM_W_HW_OBJECTINSTANCE 表的
		// OMCID，.objectmember0 获取objecttypeid 和objectno
		oi = getObjectInstanceFromTable(omcId, objFdn);
		if (oi == null) {
			return null;
		}

		String neSysid = null;
		String neName = null;
		MapHW mh = getHwMapFromTable(omcId, oi.objectTypeId, oi.objectNo);
		if (mh == null) {
			return null;
		}

		int cityId = mh.cityId;
		String version = mh.version;
		switch (neLevel) {
			case BTS_LEVEL :// bts
				neSysid = mh.neBtsId;
				neName = mh.btsName;
				break;

			case CELL_LEVEL :// cell
				neSysid = mh.neCellId;
				neName = mh.cellName;
				break;

			default :
				break;
		}

		// liangww add 2012-05-30 如果两个有一个是null,表示匹配不上
		if (Util.isNull(neSysid) || Util.isNull(neName)) {
			return null;
		}

		return new QueriedEntry(cityId, version, neSysid, neName);
	}

	/**
	 * 查找bts leven
	 * 
	 * @param omcId
	 * @param bscName
	 * @param nodeB
	 * @return
	 */
	public static QueriedEntry find3GHwBtsLeven(int omcId, String bscName, String bstId) {
		/**
		 * 具体算法：当告警ID in (22214,22216,22226)范围时，根据根据“网元名称” bscName， 关联ne_bsc_w表，得到RNC信息 提取“定位信息”字段中"NodeB标识"的值， OMCID+NE_bsc_id+NodeB标识(BTS_ID)
		 * 关联NE_BTS_W表BTS_ID，vendor=ZY0808,OMCID 得到对应的基站NE_BTS_ID
		 */
		NeBscW neBscW = getNeBscWFromTable(omcId, bscName);
		if (neBscW == null) {
			return null;
		}

		NeBtsW neBtsW = getNeBtsWFromTable(omcId, neBscW.getNeBscId(), bstId);
		// liangww add 2012-06-07 增加btsId, btsName的判断,并把getBtsId修改为getNeBtsId
		if (neBtsW == null || Util.isNull(neBtsW.getNeBtsId()) || Util.isNull(neBtsW.getBtsName())) {
			return null;
		}

		return new QueriedEntry(neBtsW.getCityId(), neBscW.getVersion(), neBtsW.getNeBtsId(), neBtsW.getBtsName());
	}

	/**
	 * 
	 * @param omcId
	 * @param neLevel
	 * @param objFdn
	 * @return
	 */
	public static QueriedEntry find2G(int omcId, int neLevel, String objFdn) {
		ObjectInstance oi = null;
		if (Util.isNull(objFdn)) {
			return null;
		}
		// 如果OBJFDN 不为空 通过 OMCID,OBJFDN 关联 CLT_PM_W_HW_OBJECTINSTANCE 表的
		// OMCID，.objectmember0 获取objecttypeid 和objectno
		oi = getObjectInstanceFromTable(omcId, objFdn);
		if (oi == null) {
			return null;
		}

		String neSysid = null;
		String neName = null;
		MapHW mh = getHwMapFromTable(omcId, oi.objectTypeId, oi.objectNo);
		if (mh == null) {
			return null;
		}

		int cityId = mh.cityId;
		String version = mh.version;
		switch (neLevel) {
			case BTS_LEVEL :// bts
				neSysid = mh.neBtsId;
				neName = mh.btsName;
				break;

			case CELL_LEVEL :// cell
				neSysid = mh.neCellId;
				neName = mh.cellName;
				break;

			default :
				break;
		}

		// liangww add 2012-05-30 如果两个有一个是null,表示匹配不上
		if (Util.isNull(neSysid) || Util.isNull(neName)) {
			return null;
		}

		return new QueriedEntry(cityId, version, neSysid, neName);
	}

	public static QueriedEntry findDevTONe3G(int omcId, String vendor, String bscId, String btsId, String cellId) {
		String sql = "select * from CFG_MAP_DEV_TO_NE t where t.omcid = %s and t.vendor='%s' ";
		StringBuilder buf = new StringBuilder();

		buf.append(String.format(sql, omcId, vendor));
		if (Util.isNotNull(bscId)) {
			buf.append(" and t.bsc_id='").append(bscId).append("' ");
		}
		if (Util.isNotNull(btsId)) {
			buf.append(" and t.bts_id='").append(btsId).append("' ");
		}
		if (Util.isNotNull(cellId)) {
			buf.append(" and t.ci='").append(cellId).append("' ");
		}

		Connection con = null;
		PreparedStatement st = null;
		ResultSet rs = null;
		double begin = System.currentTimeMillis();
		double end = 0;
		QueriedEntry queriedEntry = null;

		try {
			con = DbPool.getConn();
			st = con.prepareStatement(buf.toString());

			rs = st.executeQuery();
			if (rs.next()) {
				queriedEntry = new QueriedEntry();
				queriedEntry.cityId = rs.getInt("city_id");
				queriedEntry.version = rs.getString("version");
				queriedEntry.neBscId = rs.getString("ne_bsc_id");
				queriedEntry.neBtsId = rs.getString("ne_bts_id");
				queriedEntry.neCellId = rs.getString("ne_cell_id");
				queriedEntry.bscName = rs.getString("bsc_name");
				queriedEntry.btsName = rs.getString("bts_name");
				queriedEntry.cellName = rs.getString("cell_name");
			}
		} catch (Exception e) {
			log.warn("查询出的CFG_MAP_DEV_TO_NE异常，sql=" + sql, e);
			return null;
		} finally {
			CommonDB.close(rs, st, con);
		}

		end = System.currentTimeMillis();
		log.debug(String.format("查询CFG_MAP_DEV_TO_NE，耗时=%s秒", (end - begin) / 1000.));

		return queriedEntry;
	}

}
