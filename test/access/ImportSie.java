package access;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.PrintWriter;
import java.sql.SQLException;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;

import util.CommonDB;
import util.ExternalCmd;
import util.Util;

public class ImportSie
{
	public static void main(String[] args) throws Exception
	{
		File tmpDir = new File("d:\\sie_tmp\\");
		tmpDir.mkdirs();
		File dir = new File("C:\\Users\\ChenSijiang\\Desktop\\新建文件夹 (3)\\");
		File[] fs = dir.listFiles();
		for (File f : fs)
		{

			String tableName = f.getName().replace(".csv", "").replace("pmcomdb.", "").replace("pmdb.", "");
			InputStream in = new FileInputStream(f);
			LineIterator it = IOUtils.lineIterator(in, "utf-8");
			String head = it.nextLine();
			it.close();
			IOUtils.closeQuietly(in);
			String[] heads = head.replace("\"", "").split(",");
			String[] newHeads = new String[heads.length];
			System.arraycopy(heads, 0, newHeads, 0, newHeads.length);
			createTable(newHeads, tableName);

			File ctl = new File(tmpDir, tableName + ".ctl");
			PrintWriter pw = new PrintWriter(ctl);
			pw.println("load data");
			pw.println("CHARACTERSET ZHS16GBK");
			pw.println("infile '" + f.getAbsolutePath()
					+ "' append into table " + tableName);
			pw.println("FIELDS TERMINATED BY \",\"");
			pw.println("TRAILING NULLCOLS");
			pw.println("(");
			String[] sp = newHeads;
			// pw.print("test_id,");
			for (int i = 0; i < sp.length; i++)
			{
				String c = sp[i];
				if ( Util.isNull(c) )
					continue;
				pw.print("  " + c);
				if ( c.equals("PERIOD_START_TIME") || c.equals("LAST_MODIFIED")
						|| c.equals("TIME_STAMP") || c.equals("STARTTIME")
						|| c.equals("SVRSTARTTIME") || c.equals("ENDTIME")
						|| c.equals("SVRENDTIME") || c.equals("INSERTTIME")
						|| c.equals("INVALIDTIME") || c.equals("STARTTIME")
						|| c.equals("SVRSTARTTIME")
						|| c.equals("ENDTIME") )
				{
					pw.print(" \"to_date(:" + c + ",'YYYY-MM-DD HH24:MI:SS')\"");
				}
				if ( i < sp.length - 1 )
					pw.print(",");
				pw.println();
			}
			pw.println(")");
			pw.flush();
			pw.close();
			String cmd = "sqlldr userid=igp/igp@uway_241 skip=1 control="
					+ ctl.getAbsolutePath() + " log="
					+ ctl.getAbsolutePath().replace(".ctl", ".log");
			int ret = 0;
			ret = new ExternalCmd().execute(cmd);
			if ( ret == 0 )
			{
				ctl.delete();
				new File(ctl.getAbsolutePath().replace(".ctl", ".log")).delete();
			}
			System.out.println("ret=" + ret);
		}
	}

	static void createTable(String[] cols, String tableName)
	{
		try
		{
			CommonDB.executeUpdate("drop table " + tableName + " purge");
		}
		catch (SQLException e1)
		{
		}

		StringBuilder sb = new StringBuilder();
		sb.append("create table ").append(tableName).append("(\r\n");
		// sb.append("test_id varchar2(100),");
		for (int i = 0; i < cols.length; i++)
		{
			if ( Util.isNull(cols[i]) )
				continue;
			sb.append("    ").append(cols[i]).append(" ");
			if ( cols[i].equals("PERIOD_START_TIME")
					|| cols[i].equals("LAST_MODIFIED")
					|| cols[i].equals("TIME_STAMP")
					|| cols[i].equals("STARTTIME")
					|| cols[i].equals("SVRSTARTTIME")
					|| cols[i].equals("ENDTIME")
					|| cols[i].equals("SVRENDTIME")
					|| cols[i].equals("INSERTTIME")
					|| cols[i].equals("INVALIDTIME") )
				sb.append("date");
			else if ( cols[i].equals("OBJ_GID")||cols[i].equals("CO_GID")||cols[i].equals("BTS_GID")||cols[i].equals("BSC_GID") )
				sb.append("number");
			else if ( cols[i].endsWith("_ID") && !cols[i].equals("RWB_ID")
					&& !cols[i].equals("TR_ID") && !cols[i].equals("TRSUB_ID")
					&& !cols[i].equals("CHTYP_ID")
					&& !cols[i].equals("RBDL_ID") && !cols[i].equals("THP_ID")
					&& !cols[i].equals("TCTT_ID")
					&& !cols[i].equals("SDUBER_ERROR_ID")
					&& !cols[i].equals("RBUL_ID") && !cols[i].equals("ALU_ID")
					&& !cols[i].equals("HOT_ID")
					&& !cols[i].equals("WBTS_AUTO_CONN_SITE_ID") )
				sb.append("number");
			else
				sb.append("varchar2(200)");
			if ( i < cols.length - 1 )
				sb.append(",");
			sb.append("\r\n");
		}
		sb.append(")");
		String sql = sb.toString();
		try
		{
			CommonDB.executeUpdate(sql);
			System.out.println(tableName + "创建成功");
		}
		catch (SQLException e)
		{
			System.out.println(tableName + "创建失败");
		}
	}
}
