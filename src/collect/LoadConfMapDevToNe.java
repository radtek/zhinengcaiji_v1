package collect;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import util.CommonDB;
import util.LogMgr;

/**
 * 加载网元信息的定时器
 * 
 * @author lijiayu @ 2013年9月14日
 */
public class LoadConfMapDevToNe extends Thread {

	private static final Logger logger = LogMgr.getInstance().getSystemLogger();

	/*neMapcpmMap 防止并发用ConcurrentHashMap*/
	// 网无缓存数据 (String->SID+NID+PN+载频，采用四个字段拼接作为Map的Key)
	public static Map<String, List<Sector>> neMap = new ConcurrentHashMap<String, List<Sector>>();

	// 网无缓存数据 (Integer->PN字段作为Map的Key)
	public static Map<Integer, List<Sector>> cpmMap = new ConcurrentHashMap<Integer, List<Sector>>();

	// 是否为第一次启动标记
	private static boolean isFirstRunFlag = true;

	/**
	 * 第一次启动网元加载时
	 */
	public void starts() {
		logger.info("LoadConfMapDevToNe start.");
		// 首次启动，定时器还没到执行时间点先查询一次
		if (isFirstRunFlag)
			executeQueryNe();
		// 启动定时扫描
		this.start();
	}

	public void run() {
		// 一天的毫秒数
		long timePeriod = 24 * 60 * 60 * 1000;

		// 每天的23:59:59更新
		final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd '23:59:59'");

		// 首次运行时间
		Date startTime = null;
		try {
			startTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(sdf.format(new Date()));
		} catch (ParseException e) {
			logger.error("LoadConfMapDevToNe.err startTime error:" + e);
		}

		Timer timer = new Timer();
		// 创建TimerTask
		TimerTask timerTask = new TimerTask() {

			@Override
			public void run() {
				logger.info("LoadConfMapDevToNe timerTask strat!");
				isFirstRunFlag = false;
				executeQueryNe();
				// System.out.println(neMap.get("1314175258283"));
				logger.info("LoadConfMapDevToNe timerTask end!");
			}

		};
		// 执行计划
		timer.scheduleAtFixedRate(timerTask, startTime, timePeriod);
	}

	/**
	 * 查询网元信息并加载至
	 */
	private void executeQueryNe() {
		String sql = "select nid,sid,pn,carr,longitude,latitude,ne_bsc_id,ne_bts_id,ne_cell_id,ne_carr_id,bsc,bts,cell,carr,CELL_NAME,BSC_NAME,vendor from cfg_map_dev_to_ne";
		logger.info("load网元信息SQL为:" + sql);
		long beginTime = System.currentTimeMillis();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Connection conn = null;
		conn = CommonDB.getConnection();
		logger.info("getConnection is ok");
		// 每次加载前先清空
		neMap.clear();
		cpmMap.clear();
		try {
			String key = null;
			pstmt = conn.prepareStatement(sql);
			pstmt.setFetchSize(1000);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				StringBuffer bf = new StringBuffer();
				int pn = rs.getInt("pn");
				bf.append(rs.getInt("pn")).append(rs.getInt("carr"));
				key = bf.toString();
				List<Sector> sectorList = neMap.get(key);
				if (null == sectorList) {
					sectorList = new ArrayList<Sector>();
					neMap.put(key, sectorList);
				}
				List<Sector> sectorCpmList = cpmMap.get(pn);
				if (null == sectorCpmList) {
					sectorCpmList = new ArrayList<Sector>();
					cpmMap.put(pn, sectorCpmList);
				}
				Sector sector = new Sector();
				buildSector(sector, rs);
				sectorList.add(sector);
				sectorCpmList.add(sector);
			}
			logger.info("load网元OK,expend time:" + (System.currentTimeMillis() - beginTime));
		} catch (SQLException e) {
			logger.error("load网元信息异常error:" + e);
		} finally {
			try {
				if (rs != null)
					rs.close();
				if (pstmt != null)
					pstmt.close();
				if (conn != null)
					conn.close();
			} catch (Exception e) {
			}
		}

	}

	/**
	 * 构建 经纬度网元实体类 By ResultSet
	 * 
	 * @param sector
	 *            经纬度网元实体类
	 * @param rs
	 *            每一条记录
	 * @throws SQLException
	 */
	private void buildSector(Sector sector, ResultSet rs) throws SQLException {
		sector.setLongitude(rs.getDouble("longitude"));
		sector.setLatitude(rs.getDouble("latitude"));
		sector.setNeBscId(rs.getLong("ne_bsc_id"));
		sector.setNeBtsId(rs.getLong("ne_bts_id"));
		sector.setNeCellId(rs.getLong("ne_cell_id"));
		sector.setNeCarrId(rs.getLong("ne_carr_id"));
		sector.setBsc(rs.getInt("bsc"));
		sector.setBts(rs.getInt("bts"));
		sector.setCell(rs.getInt("cell"));
		sector.setCarr(rs.getInt("carr"));
		sector.setCellName(rs.getString("CELL_NAME"));
		sector.setBscName(rs.getString("BSC_NAME"));
		sector.setVendor(rs.getString("vendor"));
	}

	/**
	 * 
	 * @param String
	 *            key 是SID+NID+PN+载频，采用四个字段拼接作为Map的Key
	 * @param double longitude
	 * @param double latitude
	 * @return
	 */
	public static Sector getSectorByKey(String key, double longitude, double latitude) {
		List<Sector> sectorList = neMap.get(key);
		if (sectorList == null || sectorList.size() < 0)
			return null;
		return getSectorFromList(longitude, latitude, sectorList);
	}

	/**
	 * 
	 * @param Integer
	 *            Pn作为Map的Key
	 * @param double longitude
	 * @param double latitude
	 * @return
	 */
	public static Sector getSectorByPn(Integer pn, double longitude, double latitude) {
		List<Sector> sectorList = cpmMap.get(pn);
		if (sectorList == null || sectorList.size() < 0)
			return null;
		return getSectorFromList(longitude, latitude, sectorList);
	}

	private static Sector getSectorFromList(double longitude, double latitude, List<Sector> sectorList) {
		// double sum;
		// LoadConfMapDevToNe.Sector rtSector = null;
		// 先排序
		// Collections.sort(sectorList, getComparatorSector());
		// sum = longitude + latitude;
		// for (Sector sector : sectorList) {
		// rtSector = sector;
		// if (sum <= (sector.getLongitude() + sector.getLatitude()))
		// return rtSector;
		// }
		if (sectorList == null || sectorList.size() == 0)
			return null;
		int minIndex = 0;
		double minDistance = Double.MAX_VALUE;
		for (int n = 0; n < sectorList.size(); n++) {
			LoadConfMapDevToNe.Sector sector = sectorList.get(n);
			double distance = distanceOperation(longitude, latitude, sector.getLongitude(), sector.getLatitude());
			if (minDistance > distance) {
				minDistance = distance;
				minIndex = n;
			}
		}
		return sectorList.get(minIndex);
	}

	/**
	 * 两点经纬度求距离
	 * 
	 * @param lon
	 *            经度
	 * @param lat
	 *            纬度
	 * @param lon1
	 *            经度1
	 * @param lat1
	 *            纬度1
	 * @return 两点距离，单位:千米
	 */
	public static final double distanceOperation(double lon, double lat, double lon1, double lat1) {
		double latRadian = convertToRadian(lat); // 纬度转换弧度
		double lat1Radian = convertToRadian(lat1); // 纬度转换弧度
		double a = latRadian - lat1Radian; // 两点纬度弧度差
		double b = convertToRadian(lon - lon1);
		// google map 算法
		double s = 2.0 * Math.asin(Math.sqrt(Math.pow(Math.sin(a / 2.0), 2.0) + Math.cos(latRadian) * Math.cos(lat1Radian)
				* Math.pow(Math.sin(b / 2.0), 2.0)));
		return s * EQUATOR_RADIUS;
	}

	/**
	 * 角度转弧度
	 * 
	 * @param d
	 *            角度
	 * @return 弧度值
	 */
	public static final double convertToRadian(double d) {
		return d * Math.PI / 180.0;
	}

	/**
	 * 赤道半径，单位:千米
	 */
	public static final double EQUATOR_RADIUS = 6378.137;

	public static Comparator<Sector> getComparatorSector() {
		return new Comparator<Sector>() {

			@Override
			public int compare(Sector o1, Sector o2) {
				Double sum1 = o1.longitude + o1.latitude;
				Double sum2 = o2.longitude + o2.latitude;
				return sum1.compareTo(sum2);
			}

		};
	}

	/**
	 * 经纬度网元实体类
	 * 
	 * @author JerryLi616 @ 2013年9月14日
	 * @author sunt @ 2015-08-04 添加厂商字段vendor
	 */
	public class Sector {

		// 经度
		private double longitude;

		// 纬度
		private double latitude;

		private long neBscId;

		private long neBtsId;

		private long neCellId;

		private long neCarrId;

		private int bsc;

		private int bts;

		private int cell;

		private int carr;

		private String cellName;

		private String bscName;
		
		private String vendor;

		public double getLongitude() {
			return longitude;
		}

		public void setLongitude(double longitude) {
			this.longitude = longitude;
		}

		public double getLatitude() {
			return latitude;
		}

		public void setLatitude(double latitude) {
			this.latitude = latitude;
		}

		public long getNeBscId() {
			return neBscId;
		}

		public void setNeBscId(long neBscId) {
			this.neBscId = neBscId;
		}

		public long getNeBtsId() {
			return neBtsId;
		}

		public void setNeBtsId(long neBtsId) {
			this.neBtsId = neBtsId;
		}

		public long getNeCellId() {
			return neCellId;
		}

		public void setNeCellId(long neCellId) {
			this.neCellId = neCellId;
		}

		public long getNeCarrId() {
			return neCarrId;
		}

		public void setNeCarrId(long neCarrId) {
			this.neCarrId = neCarrId;
		}

		public int getBsc() {
			return bsc;
		}

		public void setBsc(int bsc) {
			this.bsc = bsc;
		}

		public int getBts() {
			return bts;
		}

		public void setBts(int bts) {
			this.bts = bts;
		}

		public int getCell() {
			return cell;
		}

		public void setCell(int cell) {
			this.cell = cell;
		}

		public int getCarr() {
			return carr;
		}

		public void setCarr(int carr) {
			this.carr = carr;
		}

		public String getCellName() {
			return cellName;
		}

		public void setCellName(String cellName) {
			this.cellName = cellName;
		}

		public String getBscName() {
			return bscName;
		}

		public void setBscName(String bscName) {
			this.bscName = bscName;
		}

		public String getVendor() {
			return vendor;
		}

		public void setVendor(String vendor) {
			this.vendor = vendor;
		}

		public String toString() {
			StringBuffer bf = new StringBuffer();
			bf.append("longitude:").append(this.longitude).append(", latitude:").append(this.latitude).append(", neBscId:").append(this.neBscId)
					.append(", neBtsId:").append(this.neBtsId).append(", neCellId:").append(this.neCellId).append(", neCarrId:")
					.append(this.neCarrId).append(", bsc:").append(this.bsc).append(", bts:").append(this.bts).append(", cell:").append(this.cell)
					.append(", carr:").append(this.carr).append(", cellName:").append(this.cellName).append(", bscName:").append(this.bscName).append(", vendor:").append(this.vendor);
			return bf.toString();
		}
	}

	public static void main(String[] args) {
		new LoadConfMapDevToNe().start();
	}

}
