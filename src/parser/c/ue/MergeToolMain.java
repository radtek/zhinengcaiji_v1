package parser.c.ue;

import java.io.File;
import java.io.FileInputStream;
import java.io.RandomAccessFile;
import java.util.Iterator;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import util.LogMgr;

/**
 * 把两份BIN文件合并在一起。2012-11-26处理龙计划问题，9月份之前的BIN文件与9月份到今天为止的BIN文件合并， 因为FTP上下载不到老的终端原始文件了。
 * 
 * @author ChenSijiang 2012-11-26
 */
public class MergeToolMain {

	private static final Logger log = LogMgr.getInstance().getSystemLogger();

	public static void main(String[] args) throws Exception {

		// 较完整的文件，以此文件为基准。指定到有IMSI_INDEX和MDN_INDEX目录的那级目录。
		File baseDirFull = new File("C:\\Users\\ChenSijiang\\Desktop\\bin文件\\");

		// 不完整的，但数据较新的文件，把它的内容往baseDirFull里合并。已有的覆盖，没有的增加。
		// 指定到有IMSI_INDEX和MDN_INDEX目录的那级目录。
		File baseDirNew = new File("E:\\datacollector_path\\cdma_ue_info\\20120906\\");

		Iterator<File> it = FileUtils.iterateFiles(baseDirNew, new String[]{"bin"}, true);
		int count = 0;
		while (it.hasNext()) {
			File binFile = it.next();
			String subDir = subDir(binFile);
			File baseFile = new File(baseDirFull, subDir);
			if (!baseFile.exists()) {
				FileUtils.copyFile(binFile, baseFile);
				log.debug("不存在的bin：" + baseFile + "，已拷贝。");
			} else {
				compareMerge(binFile, baseFile);
			}
			count++;
			if (count % 100 == 0)
				log.debug("已处理" + count + "个文件。");
		}
	}

	// 从完整BIN文件路径中截取*_INDEX那部份，
	// 比如“E:\datacollector_path\cdma_ue_info\20120906\IMSI_INDEX\00\9217.bin”，截取后是“IMSI_INDEX\00\9217.bin”
	private static String subDir(File raw) {
		String str = raw.getAbsolutePath();
		int index = str.indexOf("IMSI_INDEX");
		if (index < 0)
			index = str.indexOf("MDN_INDEX");
		if (index < 0)
			return null;
		str = str.substring(index);
		return str;
	}

	private static void compareMerge(File bin, File base) throws Exception {
		FileInputStream inBin = new FileInputStream(bin);
		RandomAccessFile rafBase = new RandomAccessFile(base, "rw");
		byte[] buff = new byte[1024];
		int ret = -1;
		int binIndex = 0;
		while ((ret = inBin.read(buff)) >= 0) {
			for (int i = 0; i < ret; i += 2) {
				byte b1 = buff[i];
				byte b2 = buff[i + 1];
				if (b1 + b2 == 0) {
					binIndex += 2;
					continue;
				}
				rafBase.seek(binIndex);
				rafBase.writeByte(b1);
				rafBase.writeByte(b2);
				binIndex += 2;
			}
		}
		rafBase.close();
		inBin.close();
	}
}
