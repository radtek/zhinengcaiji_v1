package util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import task.CollectObjInfo;
import task.RegatherObjInfo;
import util.opencsv.CSVWriter;
import framework.SystemConfig;

/**
 * 利用poi将excel文件转换为csv文件的工具(支持xls, xlsx)。
 * 
 * TODO 日期转换时有些问题，另外如果是number时，会有两位小数点---liangww
 * 
 * @author liangww 2012-09-25
 * @since 1.1
 */
public class ExcelToCsvUtil2 {

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
	public ExcelToCsvUtil2(File excelFile, CollectObjInfo taskInfo) throws FileNotFoundException {
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
	public ExcelToCsvUtil2(String excelFile, CollectObjInfo taskInfo) throws FileNotFoundException {
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

		Workbook wb = null;
		FileInputStream in = new FileInputStream(source);
		if (source.getName().endsWith(".xls")) {
			wb = new HSSFWorkbook(in);
		} else if (source.getName().endsWith(".xlsx")) {
			wb = new XSSFWorkbook(in);
		} else {
			throw new Exception("");
		}

		logger.debug(keyId + "-开始将EXCEL文件转换为CSV文件:" + source.getAbsolutePath());
		int sheetSize = wb.getNumberOfSheets();
		for (int sheetIndex = 0; sheetIndex < sheetSize; sheetIndex++) {
			Sheet sheet = wb.getSheetAt(sheetIndex);
			int rowCount = sheet.getPhysicalNumberOfRows();
			if (rowCount < 1)
				continue;

			int columnsCount = sheet.getRow(0).getPhysicalNumberOfCells();
			File csvFile = new File(dir, sheet.getSheetName() + ".csv");

			String[] cols = new String[columnsCount];
			PrintWriter writer = new PrintWriter(csvFile);
			Character sp = (splitChar == null ? ',' : splitChar);
			CSVWriter csvWriter = new CSVWriter(writer, sp);
			for (int i = 0; i < rowCount; i++) {
				Row row = sheet.getRow(i);
				for (int j = 0; j < columnsCount; j++) {
					Cell cell = row.getCell(j);
					String content = getCellContent(cell);
					content = (content) == null ? "" : content.replace('\r', ' ').replace('\n', ' ').replace(sp, ' ');
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

	static String getCellContent(Cell cell) {
		String content = null;
		if (cell == null) {
			return "";
		}

		// TODO 这没处理日期的 liangww 2012-10-16
		if (cell.getCellType() == Cell.CELL_TYPE_NUMERIC) {
			String v = String.valueOf(cell.getNumericCellValue());
			if (v != null && v.trim().length() > 2 && v.trim().endsWith(".0"))
				v = v.trim().substring(0, v.trim().length() - 2);
			return v;
		} else if (cell.getCellType() == Cell.CELL_TYPE_BOOLEAN)
			return String.valueOf(cell.getBooleanCellValue());

		return cell.toString();
		// switch (cell.getCellType())
		// {
		// case Cell.CELL_TYPE_NUMERIC:
		// content = Double.toString(cell.getNumericCellValue());
		// break;
		//
		// default:
		// content = cell.getStringCellValue();
		// break;
		// }
		//
		// return content;
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
