package util.mr;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Date;
import java.util.HashMap;

import util.Util;

public class LocalorFileSplitAdapt {

	// 文件前缀名称
	private String fileprefix;

	private int splittype;

	private int contextAppendtype;

	private BufferedWriter sourcebw;

	private HashMap<String, BufferedWriter> files = new HashMap<String, BufferedWriter>();

	private String sourceSaveFileName;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	public LocalorFileSplitAdapt(int _splittype) {
		this.splittype = _splittype;
	}

	/**
	 * 拆分定位文件成多个时段定位文件时获取文件句柄
	 * 
	 * @param report_date
	 * @return
	 */
	public BufferedWriter getSplitCalculateBw(String report_date) {
		if (this.splittype == 0)
			return this.sourcebw;
		BufferedWriter bw = null;
		String format = this.getsplitfomat(this.splittype);
		Date d = null;
		try {
			d = Util.getDate(report_date, format);
		} catch (Exception e) {
		}
		String filedate = Util.getDateString_yyyyMMddHHmmss(d);
		String newfilename = fileprefix + "_" + filedate + ".txt";
		if (!files.containsKey(newfilename)) {

			try {
				// 增加了如果定位文件已经存在,并且需要以追究方式写文件时则先判断文件是否存在,文件存在则以追加方式写文件
				File file = new File(newfilename);
				if (file.exists() && file.isFile() && file.canWrite() && this.contextAppendtype == 1)
					bw = new BufferedWriter(new FileWriter(newfilename, true));
				else
					bw = new BufferedWriter(new FileWriter(newfilename));
			} catch (Exception e) {
				System.out.println(newfilename + " 没有打开");
			}
			files.put(newfilename, bw);
		} else
			bw = (BufferedWriter) files.get(newfilename);
		return bw;
	}

	public String[] getoutputfilenames() {
		try {
			if (this.splittype == 0) {
				String[] ret = {this.sourceSaveFileName};
				return ret;
			}

			String[] keys = (String[]) files.keySet().toArray(new String[0]);

			return keys;
		} catch (Exception e) {

		}
		return null;
	}

	public void destory() {
		try {
			String[] keys = (String[]) files.keySet().toArray(new String[0]);
			for (int i = 0; i < keys.length; i++) {
				try {
					((BufferedWriter) files.get(keys[i])).close();
				} catch (Exception e) {

				}
			}
		} catch (Exception e) {

		}
	}

	private String getsplitfomat(int type) {
		String ret = "";
		switch (type) {
			case 1 :
				ret = "yyyy-MM-dd HH";
				break;
			case 2 :
				ret = "yyyy-MM-dd HH:mm";
				break;
		}
		return ret;
	}

	public int getSplittype() {
		return splittype;
	}

	public void setSplittype(int splittype) {
		this.splittype = splittype;
	}

	public int getContextAppendtype() {
		return contextAppendtype;
	}

	public void setContextAppendtype(int contextAppendtype) {
		this.contextAppendtype = contextAppendtype;
	}

	public BufferedWriter getSourcebw() {
		return sourcebw;
	}

	public void setSourcebw(BufferedWriter sourcebw) {
		this.sourcebw = sourcebw;
	}

	public String getFileprefix() {
		return fileprefix;
	}

	public void setFileprefix(String fileprefix) {
		this.fileprefix = fileprefix;
	}

	public String getSourceSaveFileName() {
		return sourceSaveFileName;
	}

	public void initsourcebw(String sourceSaveFileName) throws Exception {
		try {
			// 增加了如果定位文件已经存在,并且需要以追究方式写文件时则先判断文件是否存在,文件存在则以追加方式写文件,需要移到filesplitadapt内处理
			File file = new File(sourceSaveFileName);
			if (file.exists() && file.isFile() && file.canWrite() && this.contextAppendtype == 1)
				this.sourcebw = new BufferedWriter(new FileWriter(sourceSaveFileName, true));
			else
				this.sourcebw = new BufferedWriter(new FileWriter(sourceSaveFileName));
		} catch (Exception e) {
			throw e;
		}
	}

	public void setSourceSaveFileName(String sourceSaveFileName) {
		this.sourceSaveFileName = sourceSaveFileName;
	}

}
