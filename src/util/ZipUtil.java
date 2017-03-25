package util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

public class ZipUtil {

	private static final Logger log = LogMgr.getInstance().getSystemLogger();

	public static boolean zipDir(File dir, File target, boolean needTopDir) {
		FileOutputStream f = null;
		ZipOutputStream out = null;
		try {
			f = new FileOutputStream(target);
			out = new ZipOutputStream(f);
			if (needTopDir)
				zipFile(out, dir, dir.getName());
			else
				zipFile(out, dir, "");
			return true;
		} catch (IOException e) {
			log.error("压缩文件失败，dir=" + dir + "，target=" + target, e);
			return false;
		} finally {
			IOUtils.closeQuietly(out);
			IOUtils.closeQuietly(f);
		}
	}

	private static void zipFile(ZipOutputStream out, File srcDir, String filePath) throws IOException {
		for (File file : srcDir.listFiles()) {
			if (file.isDirectory()) {
				if (Util.isNotNull(filePath))
					zipFile(out, file, filePath + "/" + file.getName());
				else
					zipFile(out, file, file.getName());
				continue;
			}
			FileInputStream in = null;
			try {
				in = new FileInputStream(file);
				if (Util.isNotNull(filePath))
					out.putNextEntry(new ZipEntry(filePath + "/" + file.getName()));
				else
					out.putNextEntry(new ZipEntry(file.getName()));
				byte[] b = new byte[2];
				int off = 0;
				while ((in.read(b, off, 2)) != -1) {
					out.write(b);
				}
				in.close();
			} finally {
				IOUtils.closeQuietly(in);
			}
		}
	}

	public static void main(String[] args) {
		zipDir(new File("E:\\datacollector_path\\cdma_ue_info\\10000\\IMSI_INDEX"), new File("f:\\IMSI_INDEX.zip"), true);
	}
}
