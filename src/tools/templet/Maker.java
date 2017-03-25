package tools.templet;

import java.io.File;

/**
 * 模板生成工具 Maker
 * 
 * @author litp
 * @since 1.0
 */
public class Maker {

	private static TempletMaker getMaker(int type) {
		TempletMaker tMaker = null;
		if (type == 0) {
			tMaker = new DatabaseMaker();
		} else if (type == 1) {
			tMaker = new CsvFileMaker();
		}

		return tMaker;
	}

	public static void main(String[] args) throws Exception {
		int len = args.length;
		if (len != 3) {
			System.out.println("参数不正确");
			return;
		}

		// 解析类型
		String ttype = args[0];
		int iTType = -1;
		try {
			iTType = Integer.parseInt(ttype);
		} catch (Exception e) {
		}

		if (iTType < 0) {
			System.out.println("模板类型参数错误,只能是数字");
			return;
		} else if (iTType != 0 && iTType != 1) {
			System.out.println("不支持的模板类型");
			return;
		}

		// 模板参考的excel文件名
		String refFileName = args[1].trim();
		// 输出文件名
		String oFileName = args[2].trim();

		if (refFileName.length() < 2 || oFileName.length() < 1) {
			System.out.println("参数不正确");
			return;
		}

		TempletMaker tMaker = Maker.getMaker(iTType);
		tMaker.make(System.getProperty("user.dir") + File.separator + refFileName, oFileName);
	}

}
