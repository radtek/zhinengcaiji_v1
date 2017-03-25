package util;

import java.io.File;
import java.util.Collection;

import org.apache.commons.io.FileUtils;

public class Testt {
	
	public void tellMyEncoding(){
		System.out.println("我的编码是：" + System.getProperty("file.encoding"));
	}
	
	/**
	 * 去掉多余的字符串，转为数字
	 * @param fdTime
	 * @return
	 */
	public static String getNum(String fdVal) {
		if (fdVal == null)
			return "";

		if (fdVal.contains("(") && fdVal.contains(")"))
			fdVal = fdVal.substring(0, fdVal.indexOf("("));
//		if(fdVal == null)
//			return "";
		return fdVal.trim();
	}

	/**
	 * @param args
	 */
	 public static void main(String[] args) {
        try {
            String srcDirPath = "/home/yuy/workplace/igp1/igp_v1.0/test";
            // 转为UTF-8编码格式源码路径
            String utf8DirPath = "/home/yuy/workplace/igp1/igp_v1.0/test2";

            // 获取所有java文件
            Collection<File> javaGbkFileCol = FileUtils.listFiles(new File(srcDirPath), new String[] { "java" }, true);

            for (File javaGbkFile : javaGbkFileCol) {
                // UTF8格式文件路径
                String utf8FilePath = utf8DirPath + javaGbkFile.getAbsolutePath().substring(srcDirPath.length());
                // 使用GBK读取数据，然后用UTF-8写入数据
                FileUtils.writeLines(new File(utf8FilePath), "UTF-8", FileUtils.readLines(javaGbkFile, "GBK"));
            }
        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
        }

    }

}
