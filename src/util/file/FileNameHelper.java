package util.file;

import java.io.*;

/**
 * 文件名辅助类
 * 
 * @author YangJian
 * @since 1.0
 */
public class FileNameHelper {

	public static String getExt(String fileName) {
		fileName = new File(fileName).getName();
		String ext = "";
		if (fileName.lastIndexOf(".") > -1) {
			ext = fileName.substring(fileName.lastIndexOf(".") + 1);
		}
		return ext;
	}
}
