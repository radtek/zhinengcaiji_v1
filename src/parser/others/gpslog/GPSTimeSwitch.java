package parser.others.gpslog;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.log4j.Logger;

import util.LogMgr;

public class GPSTimeSwitch {

	private final static String GPSStartTime = "1980-01-06 00:00:00";// gps起点时间

	// private final static int period = 1024;//gps周期，单位（周）

	private final static int zone = 8;// 8个时区

	private static Logger logger = LogMgr.getInstance().getSystemLogger();

	public static String convertGPSTimeToLocalTime(String gpsTime) throws Exception {
		if (gpsTime == null || !gpsTime.contains(",") || gpsTime.trim().equals(""))
			return null;

		gpsTime = gpsTime.trim();

		long weeks = Integer.parseInt(gpsTime.substring(0, gpsTime.indexOf(",")));
		double seconds = Double.parseDouble((gpsTime.substring(gpsTime.indexOf(",") + 1, gpsTime.length())));
		long oneWeekTime = 7 * 24 * 60 * 60;
		int mills = 1000;

		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		// format.setTimeZone(TimeZone.getTimeZone("GMT+8"));
		try {
			long baseTime = format.parse(GPSStartTime).getTime();// 基准时间
			long weeksTime = (long) weeks * oneWeekTime * mills;// 周换算成秒
			long secondsTime = (long) seconds * mills;// 秒
			long zoneTime = (long) zone * 60 * 60 * mills;// 北京时间加8个时区
			return format.format(new Date(baseTime + weeksTime + secondsTime + zoneTime));
		} catch (Exception e) {
			logger.debug("GPSTime转换时出错", e);
		}
		return null;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		String time = "1744,252730.011";
		System.out.println(convertGPSTimeToLocalTime(time));
	}
}
