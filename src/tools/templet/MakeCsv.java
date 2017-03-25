package tools.templet;

import java.io.File;

/**
 * MakeCsv
 * 
 * @author liuwx 2010-8-17
 */
public class MakeCsv {

	public static void main(String[] args) throws Exception {
		// 目录路径
		String filePath = args[0];
		System.out.println(filePath);
		if (filePath == null || filePath.equals("")) {
			System.out.println("请设置数据文件目录路径 dir ");
			return;
		}
		// 模板参考的excel文件名
		String outfile = args[1].trim();

		// 数据文件分隔符号
		String split = args[2].trim();
		if (split == null || split.equals("")) {
			System.out.println("请设置数据文件分隔符号 split");
			return;
		}

		MakeCsbFile makefile = new MakeCsbFile();

		String var2length = args[4];

		String endcontent = "====";

		if (var2length == null || var2length.equals("")) {
			System.out.println("varchar2 类型长度错误,只能是数字 var2length");
			return;
		}
		System.out.println("var2length " + var2length);
		int varchar2Length = Integer.parseInt(var2length);
		if (varchar2Length <= 0 || varchar2Length > 4000) {
			System.out.println("varchar2 类型长度为1-4000,请重新设置");
			return;
		}

		String tableprefix = args[5];
		if (tableprefix == null || tableprefix.equals("")) {
			System.out.println("请设置采集表前缀 tableprefix");
			return;
		}
		MakeTempletCsv csv = new MakeTempletCsv(filePath, System.getProperty("user.dir") + File.separator + outfile, split, "", endcontent,
				varchar2Length, tableprefix);
		makefile.setCsv(csv);
		makefile.makeCsv();
	}

}
