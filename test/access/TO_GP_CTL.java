package access;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;

import util.CommonDB;
import util.DbPool;

class TypeLen
{
	String type;
	int len;

	public TypeLen(String type, int len)
	{
		super();
		this.type = type;
		this.len = len;
	}

}

public class TO_GP_CTL
{

	public static void main(String[] args) throws Exception
	{
		String sql = "select t.COLUMN_NAME col,t.DATA_TYPE type,t.data_length len from all_tab_cols t "
				+ "where t.TABLE_NAME='MOD_DO_STREAM_HW' and t.owner='CNOAP'";
		System.out.println(sql);
		HashMap<String, TypeLen> types = new HashMap<String, TypeLen>();

		Connection con = DbPool.getConn();
		Statement st = con.createStatement();
		ResultSet rs = st.executeQuery(sql);
		while (rs.next())
			types.put(rs.getString("col"), new TypeLen(rs.getString("type"), rs.getInt("len")));

		CommonDB.close(rs, st, con);

		System.out.println(types);

		String[] tmpCols = null;

		LineIterator it = IOUtils.lineIterator(new InputStreamReader(new FileInputStream("C:\\Documents and Settings\\ChenSijiang\\桌面\\caihf\\new\\tmp")));
		StringBuilder sb = new StringBuilder();
		while (it.hasNext())
			sb.append(it.nextLine());
		it.close();
		tmpCols = sb.toString().replace("\r", "").replace("\n", "").split(",");
		System.out.println(tmpCols);

		PrintWriter pw = new PrintWriter("C:\\Documents and Settings\\ChenSijiang\\桌面\\caihf\\new\\out.txt");
		for (String s : tmpCols)
		{
			String type = null;
			try
			{
				type = types.get(s.toUpperCase().trim()).type;
			}
			catch (Exception e)
			{
				System.out.println(s.toUpperCase().trim()); 
			}
			if ( type.equals("VARCHAR2") || type.equals("CHAR") )
				type = "character(" + types.get(s.toUpperCase().trim()).len + ")";
			else if ( type.equals("NUMBER") )
				type = "numeric";
			else if ( type.equals("DATE") )
				type = "timestamp";
			else
				type = "???";
			pw.println(s.toUpperCase().trim() + " " + type + ",");
		}
		pw.flush();
		pw.close();
	}
}
