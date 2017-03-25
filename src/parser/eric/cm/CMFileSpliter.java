package parser.eric.cm;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import util.LogMgr;

/**
 * 用于拆分大的爱立信参数文件，每个rnc一个文件。拆分出来的文件，与被拆分文件在同一目录。
 * 
 * @author ChenSijiang 2010-6-10
 */
public class CMFileSpliter {

	private File source;

	private Logger logger = LogMgr.getInstance().getSystemLogger();

	private static final String SUBNETWORK_START = "<xn:SubNetwork id=";

	private static final String SUBNETWORK_END = "</xn:SubNetwork>";

	private static final String PUBLIC_END = "</xn:SubNetwork></configData><fileFooter dateTime=\"\"/></bulkCmConfigDataFile>";

	private static final String LINE_SEPARATOR = System.getProperty("line.separator");

	public static final String eriCmSplitFileNamePrefix = "rnc_";

	/**
	 * @param source
	 *            被拆分的文件
	 */
	public CMFileSpliter(File source) {
		this.source = source;
	}

	/**
	 * 开始拆分
	 * 
	 * @return 拆分后的所有文件
	 */
	public List<File> split() {
		List<File> files = new ArrayList<File>();

		FileInputStream fis = null;
		InputStreamReader isr = null;
		BufferedReader br = null;
		PrintWriter pw = null;
		try {
			fis = new FileInputStream(source);
			isr = new InputStreamReader(fis);
			br = new BufferedReader(isr);
			String head = null;
			String line = null;
			String tmp = "";
			int i = 0;
			int rncIndex = 0;
			while ((line = br.readLine()) != null) {
				i++;
				String trimed = line.trim();
				if (head == null) {
					if (trimed.startsWith(SUBNETWORK_START)) {
						head = (tmp + trimed);
					}
					tmp += (trimed + LINE_SEPARATOR);
				} else {
					if (trimed.startsWith(SUBNETWORK_START)) {
						String name = eriCmSplitFileNamePrefix + (rncIndex++) + ".xml";
						File f = new File(source.getParent(), name);
						files.add(f);
						pw = new PrintWriter(f);
						pw.println(head);
						pw.println(trimed);
					} else if (trimed.startsWith(SUBNETWORK_END) && pw != null) {
						pw.println(trimed);
						pw.println(PUBLIC_END);
						pw.flush();
						pw.close();
						pw = null;
					} else if (pw != null) {
						pw.println(trimed);
						if (i % 100 == 0) {
							pw.flush();
						}
					}
				}
			}
		} catch (Exception e) {
			logger.error("拆分文件时异常", e);
		} finally {
			try {
				if (pw != null) {
					pw.flush();
					pw.close();
				}
				if (br != null) {
					br.close();
				}
				if (isr != null) {
					isr.close();
				}
				if (fis != null) {
					fis.close();
				}
			} catch (Exception e) {
			}
		}

		return files;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		long start = System.currentTimeMillis();
		CMFileSpliter s = new CMFileSpliter(new File("C:\\Users\\ChenSijiang\\Desktop\\All_NE_cm_exp_20100608_143156.xml"));
		List<File> fs = s.split();
		for (File f : fs) {
			System.out.println(f.getAbsolutePath());
		}
		System.out.println((System.currentTimeMillis() - start) / 1000);
	}

}
