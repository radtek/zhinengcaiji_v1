package util.net;

/**
 * IP
 * 
 * @author YangJian
 * @since 1.0
 */
public class IP {

	/**
	 * IP合法性验证
	 * 
	 * @param ipAddr
	 *            ip地址
	 * @return
	 */
	public static boolean isValidIpAddress(String ipAddr) {
		if (ipAddr == null || ipAddr.length() < 7)
			return false;

		String[] parts = ipAddr.split("[.]");
		if (parts.length != 4) {
			return false;
		}

		for (int i = 0; i < parts.length; i++) {
			int ipart = -1;
			try {
				ipart = Integer.parseInt(parts[i]);
			} catch (NumberFormatException e) {
				return false;
			}
			if (i == 0 && ipart <= 0) // 第一个元素不能为0，其余的可以为0
				return false;
			if (ipart < 0 || ipart > 255) {
				return false;
			}
		}

		return true;
	}

	/**
	 * 将字符串形式的IP地址转换成十进制整数
	 */
	public static long ip2Long(String strIp) {
		long[] ip = new long[4];
		// 先找到IP地址字符串中.的位置
		int position1 = strIp.indexOf(".");
		int position2 = strIp.indexOf(".", position1 + 1);
		int position3 = strIp.indexOf(".", position2 + 1);

		// 将每个.之间的字符串转换成整型
		ip[0] = Long.parseLong(strIp.substring(0, position1));
		ip[1] = Long.parseLong(strIp.substring(position1 + 1, position2));
		ip[2] = Long.parseLong(strIp.substring(position2 + 1, position3));
		ip[3] = Long.parseLong(strIp.substring(position3 + 1));

		return (ip[0] << 24) + (ip[1] << 16) + (ip[2] << 8) + ip[3];
	}

	/**
	 * 将十进制整数形式转换成字符串形式的ip地址
	 */
	public static String long2IP(long longIp) {
		StringBuffer sb = new StringBuffer("");
		// 直接右移24位
		sb.append(String.valueOf((longIp >>> 24)));
		sb.append(".");
		// 将高8位置0，然后右移16位
		sb.append(String.valueOf((longIp & 0x00FFFFFF) >>> 16));
		sb.append(".");
		// 将高16位置0，然后右移8位
		sb.append(String.valueOf((longIp & 0x0000FFFF) >>> 8));
		sb.append(".");
		// 将高24位置0
		sb.append(String.valueOf((longIp & 0x000000FF)));
		return sb.toString();
	}

	// 单元测试
	public static void main(String[] args) {
		System.out.println(IP.isValidIpAddress("127.x.0.1"));
		System.out.println(IP.ip2Long("192.168.0.1"));
		System.out.println(IP.long2IP(3232235521L));
	}
}
