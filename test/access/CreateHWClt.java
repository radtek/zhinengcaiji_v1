package access;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;

public class CreateHWClt
{
	public static void main(String[] args) throws Exception
	{
		File dir = new File("C:\\Documents and Settings\\ChenSijiang\\桌面\\北京_华为配置参数\\clt_cm_hw\\");
		File[] fs = dir.listFiles();
		PrintWriter pw = new PrintWriter(new File("C:\\Documents and Settings\\ChenSijiang\\桌面\\output.txt"));
		for (File f : fs)
		{
			String s = extraFile(f);
			pw.println(s);
			pw.flush();
		}
		pw.close();
	}

	static String extraFile(File file) throws Exception
	{
		BufferedReader br = new BufferedReader(new FileReader(file));
		StringBuilder buff = new StringBuilder();
		String line = null;
		boolean isCreateTable = false;
		boolean isEnd = false;
		while ((line = br.readLine()) != null)
		{
			line = line.trim();
			if ( line.startsWith("create table") )
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
				buff.append(line).append("\r\n");
			}
			else if ( isCreateTable && isEnd )
			{
				buff.append(line).append("\r\n");

				isCreateTable = false;
				isEnd = false;
			}

		}
		br.close();

		return buff.toString();
	}
}
