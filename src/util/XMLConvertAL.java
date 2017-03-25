package util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;

/**
 * 阿郎WCDMA性能文件转换器。 原始文件虽然是XML格式，但是整个文件，只有一行，无回车及换行符。此转换器将XML头转为一行，并将DOCTYPE节点删除。
 * 
 * @author ChenSijiang 2010-08-17
 */
public class XMLConvertAL {

	public static File convert(String fileName) throws Exception {
		return convert(new File(fileName));
	}

	public static File convert(File file) throws Exception {
		FileInputStream fis = null;
		BufferedReader br = null;
		InputStreamReader isr = null;
		File result = new File(file.getAbsoluteFile() + ".tmp");
		PrintWriter pw = null;
		try {
			pw = new PrintWriter(result);
			fis = new FileInputStream(file);
			isr = new InputStreamReader(fis);
			br = new BufferedReader(isr);
			StringBuilder buffer = new StringBuilder();
			boolean cleared = false;
			char[] cs = new char[65535];
			while (br.read(cs) > -1) {
				buffer.append(cs);
				if (!cleared) {
					int right = buffer.indexOf("?>");
					buffer.insert(right + 2, '\n');
					int mdc = buffer.indexOf("<mdc");
					buffer.delete(right + 3, mdc);
					cleared = true;
				}
				pw.print(buffer.toString().trim());
				pw.flush();
				buffer.delete(0, buffer.length());
			}
		} catch (Exception e) {
		} finally {
			try {
				if (fis != null) {
					fis.close();
				}
				if (isr != null) {
					isr.close();
				}
				if (br != null) {
					br.close();
				}
				if (pw != null) {
					pw.flush();
					pw.close();
				}
			} catch (Exception e) {
			}
		}
		if (!file.delete()) {
			throw new Exception("源文件删失败，可能被占用:" + file.getAbsolutePath());
		}
		if (!result.renameTo(file)) {
			throw new Exception("给临时文件改名时异常:" + result.getAbsolutePath() + " rename to " + file.getAbsolutePath());
		} else {
			result = file;
		}
		return result;
	}

	public static void main(String[] args) {
		try {
			File f = XMLConvertAL.convert("C:\\Users\\ChenSijiang\\Desktop\\A20100801.0300+0800-0400+0800_NodeB-DQ1_0417rangqulishuisanqiW_BOB");
			System.out.println(f.getAbsolutePath());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
