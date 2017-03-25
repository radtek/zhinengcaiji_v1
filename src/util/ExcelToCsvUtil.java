package util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import jxl.Cell;
import jxl.Sheet;
import jxl.Workbook;

import org.apache.log4j.Logger;

import task.CollectObjInfo;
import task.RegatherObjInfo;
import util.opencsv.CSVWriter;
import framework.SystemConfig;

/**
 * 将excel文件转换为csv文件的工具。
 * 
 * @author ChenSijiang 2010-10-19
 * @since 1.1
 */
public class ExcelToCsvUtil {

	private File source;

	private String keyId;

	private long taskId;

	private Timestamp dataTime;

	private Logger logger = LogMgr.getInstance().getSystemLogger();

	/**
	 * 构造方法。
	 * 
	 * @param excelFile
	 *            要转换的excel文件
	 * @param taskInfo
	 *            所属任务的信息
	 * @throws FileNotFoundException
	 *             传入参数为<code>null</code>、文件不存在、非文件时
	 */
	public ExcelToCsvUtil(File excelFile, CollectObjInfo taskInfo) throws FileNotFoundException {
		super();
		long id = taskInfo.getTaskID();
		taskId = id;
		if (taskInfo instanceof RegatherObjInfo)
			id = taskInfo.getKeyID() - 10000000;
		keyId = taskInfo.getTaskID() + "-" + id;
		dataTime = taskInfo.getLastCollectTime();
		if (excelFile == null) {
			throw new FileNotFoundException(keyId + "-传入的文件路径为null");
		}
		if (!excelFile.exists()) {
			throw new FileNotFoundException(keyId + "-文件不存在:" + excelFile.getAbsolutePath());
		}
		if (excelFile.isDirectory()) {
			throw new FileNotFoundException(keyId + "-传入的路径为目录，非文件:" + excelFile.getAbsolutePath());
		}
		this.source = excelFile;
	}

	/**
	 * 构造方法。
	 * 
	 * @param excelFile
	 *            要转换的excel文件
	 * @param taskInfo
	 *            所属任务的信息
	 * @throws FileNotFoundException
	 *             传入参数为<code>null</code>、文件不存在、非文件时
	 */
	public ExcelToCsvUtil(String excelFile, CollectObjInfo taskInfo) throws FileNotFoundException {
		this(new File(excelFile), taskInfo);
	}

	/**
	 * 将EXCEL文件转为一个或多个CSV文件，使用逗号作为分隔符。
	 * 
	 * @return 转换后的所有CSV文件的本地路径，不会返回<code>null</code>
	 * @throws Exception
	 *             转换时出错
	 */
	public List<String> toCsv() throws Exception {
		return toCsv(null);
	}

	/**
	 * 将EXCEL文件转为一个或多个CSV文件。
	 * 
	 * @param splitChar
	 *            CSV文件的分隔符，如果传入<code>null</code>，则默认使用逗号分隔
	 * @return 转换后的所有CSV文件的本地路径，不会返回<code>null</code>
	 * @throws Exception
	 *             转换时出错
	 */
	public List<String> toCsv(Character splitChar) throws Exception {
		List<String> ret = new ArrayList<String>();
		File dir = new File(SystemConfig.getInstance().getCurrentPath() + File.separator + taskId + File.separator
				+ Util.getDateString_yyyyMMddHH(dataTime) + File.separator + source.getName() + File.separator);
		if (!dir.exists()) {
			if (!dir.mkdirs()) {
				throw new Exception(keyId + "-创建文件夹失败:" + dir.getAbsolutePath());
			}
		}

		Workbook wb = Workbook.getWorkbook(source);
		logger.debug(keyId + "-开始将EXCEL文件转换为CSV文件:" + source.getAbsolutePath());
		Sheet[] sheets = wb.getSheets();
		for (Sheet sheet : sheets) {
			File csvFile = new File(dir, sheet.getName() + ".csv");
			int columnsCount = sheet.getRow(0).length;
			int rowCount = sheet.getRows();
			if (rowCount < 1)
				continue;
			String[] cols = new String[columnsCount];
			PrintWriter writer = new PrintWriter(csvFile);
			Character sp = (splitChar == null ? ',' : splitChar);
			CSVWriter csvWriter = new CSVWriter(writer, sp);
			for (int i = 0; i < rowCount; i++) {
				Cell[] cells = sheet.getRow(i);
				for (int j = 0; j < columnsCount; j++) {
					String content = j > cells.length - 1 ? "" : cells[j].getContents();
					content = content == null ? "" : content.replace('\r', ' ').replace('\n', ' ').replace(sp, ' ');
					cols[j] = content;
				}
				csvWriter.writeNext(cols);
				csvWriter.flush();
			}
			csvWriter.close();
			writer.close();
			ret.add(csvFile.getAbsolutePath());
			// logger.debug(keyId + "-CSV文件已转换完成:" + csvFile.getAbsolutePath());
		}
		logger.debug(keyId + "-CSV文件已转换完成,所在目录:" + dir.getAbsolutePath());
		return ret;
	}

	public static void main(String[] args) throws Exception {
		CollectObjInfo c = new CollectObjInfo(998);
		c.setLastCollectTime(new Timestamp(Util.getDate1("2010-10-15 00:00:00").getTime()));
		ExcelToCsvUtil u = new ExcelToCsvUtil("F:\\ftproot\\test.xls", c);
		List<String> list = u.toCsv();
		for (String s : list) {
			System.out.println(s);
		}
	}
}
