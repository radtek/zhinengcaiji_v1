package tools.templet;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MakeCsbFile {

	private FileWriter fw = null;

	private static String head = "厂家原始文件名,厂家原始文件字段,采集表,采集字段,采集字段类型,采集字段长度,是否允许为空,主键标识";

	private static String dateReg = "\\d{1,4}-\\d{1,2}-\\d{1,2}\\s+\\d{1,2}:\\d{1,2}:\\d{1,2}";

	private MakeTempletCsv csv;

	public MakeCsbFile() {

	}

	public MakeTempletCsv getCsv() {
		return csv;
	}

	public void setCsv(MakeTempletCsv csv) {
		this.csv = csv;
	}

	public void makeCsv() {
		File f = new File(csv.getFilePath());
		if (!f.exists()) {
			System.out.println("文件不存在");
			return;
		}
		try {
			File ff = new File(csv.getOutfile());
			if (!ff.exists())
				ff.createNewFile();
			fw = new FileWriter(ff);
			fw.write(head + "\r\n");
		} catch (IOException e) {
			e.printStackTrace();
		}

		boolean ftpflag = false;
		String ftpPath = csv.getFtpPath();
		if (ftpPath != null && !ftpPath.equals("")) {
			ftpflag = true;
		}

		if (f.isDirectory()) {
			File[] fs = f.listFiles();
			for (File fl : fs) {
				String name = fl.getAbsolutePath();
				if (ftpflag) {
					if (ftpPath.charAt(ftpPath.length() - 1) != '/')
						ftpPath = ftpPath + "/";
					System.out.println(ftpPath + fl.getName() + ";");
				}
				parse(name);
			}
		}
		try {
			fw.flush();
			fw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public void parse(String filename) {
		String split = csv.getSplit();
		String endContent = csv.getEndcontent();
		int varchar2Length = csv.getVarchar2Length();
		String tableprefix = csv.getTableprefix();
		try {
			InputStreamReader reader = new InputStreamReader(new FileInputStream(filename));
			BufferedReader br = new BufferedReader(reader);
			String firstline = br.readLine();
			firstline = firstline.trim();
			String firstdataline = br.readLine();
			// System.out.println(firstline);
			String[] ss = firstline.split(split);

			String sub = filename.substring(filename.lastIndexOf("\\") + 1);

			String fPath = sub;

			String[] firsts = null;
			if (firstdataline != null && !firstdataline.contains(endContent)) {
				firsts = firstdataline.split(split);
			}
			String tablename = null;
			String tm = null;
			if (sub.indexOf("_") != -1) {
				tablename = sub.substring(0, sub.indexOf("_"));
				tablename = filename.substring(filename.lastIndexOf("\\") + 1, filename.lastIndexOf("."));
				// fw.write(firstline + "\r\n" + firstdataline + "\r\n");
				tablename = tablename.substring(tablename.indexOf("_") + 1);

				tablename = tablename.substring(0, tablename.indexOf("_"));
				tablename = tablename.toUpperCase();

			} else
				tablename = sub.substring(0, sub.indexOf("."));

			tm = tableprefix + tablename;

			tablename = tableprefix + tablename;
			if (tablename.length() > 30) {
				tablename = tablename.substring(0, 30);

				System.out.println(" 表映射关系" + tm + " :" + tablename.substring(0, 30));
			}

			boolean b = false;
			int last = ss.length - 1;
			String var2Len = String.valueOf(varchar2Length);
			try {
				for (int j = 0; j < last; j++) {
					String s = ss[j];

					if (firsts != null && firsts[j] != null) {
						b = findByRegex(firsts[j], dateReg, 0);
					}
					fw.write(fPath + " , " + s + "," + tablename + "," + s + "," + (b ? " DATE " : "VARCHAR2 ") + "," + (b ? " " : var2Len) + ",Y "
							+ "\r\n");
					b = false;

				}
				if (firsts != null && firsts[last] != null) {
					b = findByRegex(firsts[last], dateReg, 0);
				}

				fw.write(fPath + " , " + ss[last] + "," + tablename + "," + ss[last] + "," + (b ? " DATE " : "VARCHAR2 ") + "," + (b ? " " : var2Len)
						+ ",Y " + "\r\n");

				fw.flush();
				reader.close();
				br.close();

			} catch (Exception e) {
				System.out.println("  生成csv  模板文件错误  ");
				e.printStackTrace();
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	// 通过正则表达式查找
	private static boolean findByRegex(String str, String regEx, int group) {
		if (regEx == null || (regEx != null && "".equals(regEx.trim()))) {
			return false;
		}
		Pattern p = Pattern.compile(regEx);
		Matcher m = p.matcher(str);

		boolean result = m.find();// 查找是否有匹配的结果
		return result;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
	}
}
