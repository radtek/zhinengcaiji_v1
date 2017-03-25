/**
 * 
 */
package util.mr;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import util.CommonDB;

/**
 * 描述：根据LAC+CI在ne_cell_g中查找city_id和ne_sys_id并填入site_database.txt文件
 * 
 * @author xumg
 * @version 1.0
 */
public class SitebaseAddInfo {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		if (args.length < 2) {
			System.out.println("参数不足。");
			return;
		}

		// 第0个参数即原始sitebase文件
		String strOriSitebase = args[0];
		// 第1个参数为修改后的sitebase文件
		String strNewSitebase = args[1];

		// 连接数据库
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		// 打开文件
		RandomAccessFile fSitebaseIn = null;
		RandomAccessFile fSitebaseOut = null;
		try {
			fSitebaseIn = new RandomAccessFile(strOriSitebase, "r");
			fSitebaseOut = new RandomAccessFile(strNewSitebase, "rw");
			fSitebaseOut.setLength(0);
		} catch (FileNotFoundException exp) {
			exp.printStackTrace();
		} catch (IOException exp) {
			exp.printStackTrace();
		}

		String strLine = null;
		try {
			conn = CommonDB.getConnection();
			// 写入行头
			strLine = fSitebaseIn.readLine();
			fSitebaseOut.writeBytes(strLine);
			fSitebaseOut.writeBytes("\tCITY_ID\tNE_SYS_ID\r\n");

			// 逐行处理
			while ((strLine = fSitebaseIn.readLine()) != null) {
				fSitebaseOut.writeBytes(strLine);
				String[] fields = strLine.split("\t");

				// 获得LAC和CI
				String strLAC = fields[4];
				String strCI = fields[0];

				try {
					// 从ne_cell_g 中查询city_id和ne_sys_id
					pstmt = conn.prepareStatement("select CITY_ID,NE_SYS_ID from NE_CELL_G where LAC=" + strLAC + " and CI=" + strCI);
					rs = pstmt.executeQuery();
					if (rs.next()) {
						// 取得city_id和ne_sys_id 的值
						int iCityID = rs.getInt("CITY_ID");
						String strSysID = rs.getString("NE_SYS_ID");

						// 写入文件
						fSitebaseOut.writeBytes("\t");
						fSitebaseOut.writeBytes(Integer.toString(iCityID));
						fSitebaseOut.writeBytes("\t");
						fSitebaseOut.writeBytes(strSysID);
					} else {
						fSitebaseOut.writeBytes("\t0\t0");
					}

					rs.close();
					pstmt.close();
				} catch (SQLException exp) {
					exp.printStackTrace();
				}

				fSitebaseOut.writeBytes("\r\n");
			}

			// 关闭文件
			fSitebaseIn.close();
			fSitebaseOut.close();
		} catch (IOException exp) {
			exp.printStackTrace();
		} finally {
			try {
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
}
