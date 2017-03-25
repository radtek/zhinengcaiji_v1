package util.mr;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import util.CommonDB;
import util.Parsecmd;
import util.Util;

/**
 * 读取NE_CELL_G中的数据，自动生成sitebase文件
 * 
 * @author xumg
 */
public class SitebaseGen {

	private String city_id;

	public void createSitebase() {
		// TODO Auto-generated method stub
		// 连接数据库
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		// 打开文件
		RandomAccessFile fSitebase = null;
		String city_id = "";
		String strFileName = "sitetmp" + File.separator + "site_database.txt";
		File file = null;

		try {
			file = new File("sitetmp");
			if (!file.exists())
				file.mkdir();
			file = new File(strFileName);
			if (file.exists()) {
				// 文件存在则删除
				file.delete();
			}

			// 创建新文件
			fSitebase = new RandomAccessFile(strFileName, "rw");
		} catch (FileNotFoundException exp) {
			exp.printStackTrace();
		}

		try {
			conn = CommonDB.getConnection();
			// 写入行头
			// String strTitle =
			// "CI\tLONGTITUDE\tLATITUDE\t频段(M)\tLAC\t天线方位角\tBSIC\tBCCH\t天线半功率角\t天线高度\t发射满功率(db)\t基站类型\tnValue\tKEYX\tCITY_ID\tNE_SYS_ID\r\n";
			String strTitle = "CI\tLONGTITUDE\tLATITUDE\tCELL_FRES\tLAC\tANT_AZIMUTH\tBSIC\tBCCH\t0\tANT_HIGH\t0\tSITE_TYPE\tnValue\tKEYX\tCITY_ID\tNE_SYS_ID\r\n";
			fSitebase.write(strTitle.getBytes());

			try {
				// 从ne_cell_g 中查询所需信息
				pstmt = conn.prepareStatement("select * from NE_CELL_G "
						+ (city_id == null || "".equals(city_id) ? "" : "where city_id = " + city_id));
				System.out.println("select * from NE_CELL_G " + (city_id == null || "".equals(city_id) ? "" : "where city_id = " + city_id));
				rs = pstmt.executeQuery();

				while (rs.next()) {
					// 取每个字段并以字符串的形式写入文件
					String strBsic = rs.getString("BSIC");
					if (Util.isNull(strBsic))
						continue;
					String strSysID = rs.getString("NE_SYS_ID");
					if (Util.isNull(strSysID))
						continue;
					int iCI = rs.getInt("CI");
					fSitebase.writeBytes(Integer.toString(iCI) + "\t");

					double dLong = rs.getDouble("LONGITUDE");
					fSitebase.writeBytes(Double.toString(dLong) + "\t");

					double dLati = rs.getDouble("LATITUDE");
					fSitebase.writeBytes(Double.toString(dLati) + "\t");

					int iFreq = rs.getInt("CELL_FRES");
					if (iFreq == 1)
						fSitebase.writeBytes("900\t");
					else
						fSitebase.writeBytes("1800\t");

					int iLac = rs.getInt("LAC");
					fSitebase.writeBytes(Integer.toString(iLac) + "\t");

					int iAntAngle = rs.getInt("ANT_AZIMUTH");
					fSitebase.writeBytes(Integer.toString(iAntAngle) + "\t");

					fSitebase.writeBytes(strBsic + "\t");

					int iBcch = rs.getInt("BCCH");
					fSitebase.writeBytes(Integer.toString(iBcch) + "\t");

					// 天线半功率角暂时未用
					fSitebase.writeBytes("0\t");

					int iAntHigh = rs.getInt("ANT_HIGH");
					fSitebase.writeBytes(Integer.toString(iAntHigh) + "\t");

					// 发射满功率暂时未用
					fSitebase.writeBytes("0\t");

					int iCellType = 0;
					try {
						iCellType = rs.getInt("SITE_TYPE");
					} catch (Exception e) {

					}
					fSitebase.writeBytes(Integer.toString(SitebaseGen.trunsitetype(iCellType)) + "\t");

					// nValue
					fSitebase.writeBytes("4.5\t");

					// KEYX
					fSitebase.writeBytes("0.35\t");

					int iCityID = rs.getInt("CITY_ID");
					fSitebase.writeBytes(Integer.toString(iCityID) + "\t");

					fSitebase.writeBytes(strSysID + "\r\n");
					// fSitebase.writeBytes("\r\n" );
				}

				// rs.close();
				// pstmt.close();
				// conn.close();
			} catch (SQLException exp) {
				exp.printStackTrace();
			}

			Parsecmd.movefile(file.getPath(), "");
		} catch (IOException exp) {
			exp.printStackTrace();
		} finally {
			try {
				// 关闭文件
				if (fSitebase != null)
					fSitebase.close();

				if (rs != null)
					rs.close();
				if (pstmt != null)
					pstmt.close();
				if (conn != null)
					conn.close();

			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		SitebaseGen sitegen = new SitebaseGen();
		if (args.length >= 1)
			sitegen.city_id = args[0];

		sitegen.createSitebase();
	}

	/**
	 * 获得sitetype值 2代表微蜂窝（2）， 别的认为是宏蜂窝（1） 从数据库里获得,如果输入不为2时统一转换为1
	 * 
	 * @param sitetype
	 * @return
	 */
	private static int trunsitetype(int sitetype) {
		if (sitetype != 2)
			return 1;
		else
			return 2;

	}

	public String getCity_id() {
		return city_id;
	}

	public void setCity_id(String city_id) {
		this.city_id = city_id;
	}
}
