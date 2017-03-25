package access;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintWriter;

public class To10G
{
	public static void main(String[] args) throws Exception
	{
		BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\ChenSijiang\\Desktop\\所有采集表结构\\诺西参数16张表\\clt_cm_w_sie_.sql"));
		PrintWriter pw = new PrintWriter("f:\\nsn_cm_w.txt");
		String line = null;
		boolean isCreateTable = false;
		boolean isEnd = false;
		while ((line = br.readLine()) != null)
		{
			line = line.trim();
			if ( line.startsWith("CREATE TABLE") )
			{
				isCreateTable = true;
			}
			else if ( line.equals(")") && isCreateTable )
			{
				isEnd = true;
				line = ");";
			}

			if ( isCreateTable && !isEnd )
			{
				pw.println(line);
				pw.flush();
			}
			else if ( isCreateTable && isEnd )
			{
				pw.println(line);
				pw.flush();
				isCreateTable = false;
				isEnd = false;
			}

		}

		pw.flush();
		pw.close();
	}
}
