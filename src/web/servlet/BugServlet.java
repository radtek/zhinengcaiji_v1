package web.servlet;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import jxl.Workbook;
import jxl.write.Label;
import jxl.write.WritableSheet;
import jxl.write.WritableWorkbook;
import jxl.write.WriteException;
import jxl.write.biff.RowsExceededException;

import org.apache.log4j.Logger;

import db.dao.TaskDAO;
import db.dao.TempletDAO;
import db.pojo.Task;
import db.pojo.Templet;
import framework.SystemConfig;

import util.DbPool;
import util.LogMgr;
import util.file.FileUtil;

/**
 * BugServlet
 * 
 * @author yuanxf
 * @since 1.0
 */
public class BugServlet extends HttpServlet {

	private static final long serialVersionUID = 1L;

	protected Logger logger = LogMgr.getInstance().getSystemLogger();

	private final String downloadPaht = "." + File.separator + "temp" + File.separator + "uwayCollectLog";

	private final String zipRootPath = "." + File.separator + "temp";

	private final String zipName = "uwayCollectLog.zip";

	private final String zipPaht = zipRootPath + File.separator + zipName;

	private final String temExl = "template.xls";

	private final String taskExl = "task.xls";

	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		doPost(req, resp);
	}

	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		String rootPath = (String) System.getProperties().get("user.dir");
		// 创建临时文件
		createFile();

		// 采集日志copy到临时文件
		String oldLogPath = rootPath + File.separator + "log";
		FileUtil.copyDir(downloadPaht, oldLogPath);

		// 配置文件copy到临时文件
		String conf = rootPath + File.separator + "conf" + File.separator + "config.xml";
		String newPath = downloadPaht + File.separator + "config.xml";
		File f = new File(newPath);
		if (!f.exists()) {
			f.createNewFile();
		}
		copy(conf, newPath);

		// 把模板表导入EXL放入临时文件
		createTemExl();

		// 把任务表导入EXL放入临时文件
		createTaskExl();

		// 把模板文件导入临时文件
		List<String> list = getTemPFileName();
		String templetPath = SystemConfig.getInstance().getTempletPath();
		for (String fName : list) {
			if (fName != null && fName.equals("")) {
				String oldTemPath = templetPath + File.separator + fName;
				String newTemPath = downloadPaht + File.separator + fName;
				File temF = new File(newTemPath);
				if (!temF.exists()) {
					temF.createNewFile();
				}
				copy(oldTemPath, newTemPath);
			}

		}

		// 压缩临时文件
		compress(downloadPaht, zipPaht);

		// 下载
		try {
			downLoad(req, resp, zipName);
		} catch (Exception e) {
			logger.error("下载采集日志失败,原因:", e);
		}
		// 删除
		File downFile = new File(downloadPaht);
		deleteFile(downFile);
		File zipFile = new File(zipPaht);
		if (zipFile.exists()) {
			zipFile.delete();
		}

	}

	/**
	 * 创建一个临时文件
	 * 
	 * @throws IOException
	 */
	private void createFile() throws IOException {
		File f = new File(downloadPaht);
		if (!f.exists()) {
			boolean bool = f.mkdir();
			if (bool) {
				logger.debug("创建下载日志的临时文件成功.");
			} else {
				logger.debug("创建下载日志的临时文件失败.");
			}
		} else {
			logger.debug("创建下载日志的临时文件已存在.");
		}

	}

	/**
	 * 下载文件到客户端
	 * 
	 * @param request
	 *            ：HttpServletRequest
	 * @param response
	 *            ：HttpServletResponse
	 * @param filename
	 *            ：要下载的文件名
	 * @throws Exception
	 */
	private void downLoad(HttpServletRequest request, HttpServletResponse response, String filename) throws Exception {
		// 文件的位置
		String path = zipPaht;
		// 以流的形式下载文件。
		File file = new File(path);
		InputStream fis = new BufferedInputStream(new FileInputStream(path));
		byte[] buffer = new byte[fis.available()];
		fis.read(buffer);
		fis.close();
		// 清空response
		response.reset();
		// 设置response的Header
		response.addHeader("Content-Disposition", "attachment;filename=" + new String(filename.getBytes()));
		response.addHeader("Content-Length", "" + file.length());
		OutputStream toClient = new BufferedOutputStream(response.getOutputStream());
		response.setContentType("application/octet-stream");
		toClient.write(buffer);
		toClient.flush();
		toClient.close();
	}

	/** */
	/**
	 * 递归压缩文件
	 * 
	 * @param source
	 *            源路径,可以是文件,也可以目录
	 * @param destinct
	 *            目标路径,压缩文件名
	 * @throws IOException
	 */
	private static void compress(String source, String destinct) throws IOException {
		List<File> fileList = loadFilename(new File(source));
		ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(new File(destinct)));

		byte[] buffere = new byte[8192];
		int length;
		BufferedInputStream bis;

		for (int i = 0; i < fileList.size(); i++) {
			File file = (File) fileList.get(i);
			zos.putNextEntry(new ZipEntry(getEntryName(source, file)));
			bis = new BufferedInputStream(new FileInputStream(file));

			while (true) {
				length = bis.read(buffere);
				if (length == -1)
					break;
				zos.write(buffere, 0, length);

			}
			bis.close();
			zos.closeEntry();
		}
		zos.close();
	}

	/** */
	/**
	 * 获得zip entry 字符串
	 * 
	 * @param base
	 * @param file
	 * @return
	 */
	private static String getEntryName(String base, File file) {
		File baseFile = new File(base);
		String filename = file.getPath();
		// int index=filename.lastIndexOf(baseFile.getName());
		if (baseFile.getParentFile().getParentFile() == null)
			return filename.substring(baseFile.getParent().length());
		return filename.substring(baseFile.getParent().length() + 1);
	}

	/** */
	/**
	 * 递归获得该文件下所有文件名(不包括目录名)
	 * 
	 * @param file
	 * @return
	 */
	private static List<File> loadFilename(File file) {
		List<File> filenameList = new ArrayList<File>();
		if (file.isFile()) {
			filenameList.add(file);
		}
		if (file.isDirectory()) {
			for (File f : file.listFiles()) {
				filenameList.addAll(loadFilename(f));
			}
		}
		return filenameList;
	}

	private static void copy(String source, String destinct) throws IOException {
		File inputFile = new File(source);
		File outputFile = new File(destinct);

		FileReader in = new FileReader(inputFile);
		FileWriter out = new FileWriter(outputFile);
		int c;

		while ((c = in.read()) != -1)
			out.write(c);

		in.close();
		out.close();
	}

	// 生成template表exl
	public void createTemExl() {
		try {
			// 生成一个Excel文件
			File file = new File(downloadPaht + File.separator + temExl);
			WritableWorkbook wwb = Workbook.createWorkbook(file);
			// 生成名为第一页的工作表,参数0表示这是第一页
			WritableSheet sheet = wwb.createSheet("第一页", 0);
			TempletDAO dao = new TempletDAO();
			List<Templet> list = dao.list();
			List<String> sqlfield = dao.getMetaData("IGP_CONF_TEMPLET");

			// 声明一个保存数字的单元格,使用Number的完整包路径,写全包名,防止与java.lang包的Number发生冲突
			jxl.write.Number id;
			jxl.write.Number type;
			// 声明Label对象的引用变量
			Label tempname;
			Label edition;
			Label filename;
			Label tablefield;
			// i与j用于计数
			int i = 0;
			int j = 0;
			// 遍历集合中的元素
			Iterator<Templet> it = list.iterator();
			Iterator<String> itfield = sqlfield.iterator();
			for (int num = 0; num <= list.size(); num++) {

				// String field ="";
				if (j == 0) {
					// 添加表字段
					for (int n = 0; n < sqlfield.size(); n++) {
						tablefield = new Label(n, j, itfield.next());
						sheet.addCell(tablefield);
					}
				} else {
					Templet tem = it.next();
					id = new jxl.write.Number(i++, j, tem.getTmpID()); // 第一列,第一行
					type = new jxl.write.Number(i++, j, tem.getTmpType()); // 将第二列,第一行
					tempname = new Label(i++, j, tem.getTmpName()); // 将第三列,第一行
					edition = new Label(i++, j, tem.getEdition()); // 将第四列,第一行
					filename = new Label(i++, j, tem.getTempFileName()); // 将第五列,第一行
					// 将定义好的单元格添加到工作表中
					sheet.addCell(id);
					sheet.addCell(type);
					sheet.addCell(tempname);
					sheet.addCell(edition);
					sheet.addCell(filename);
				}
				/** * i = 0; 那么下次循环后开始还是第一列* j++; 那么下次循环后开始就是第二行 */
				i = 0;
				j++;

			}
			// 写入数据并关闭文件
			wwb.write();
			wwb.close();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (RowsExceededException e) {
			e.printStackTrace();
		} catch (WriteException e) {
			e.printStackTrace();
		}

	}

	// 生成template表exl
	public void createTaskExl() {
		try {
			// 生成一个Excel文件
			File file = new File(downloadPaht + File.separator + taskExl);
			WritableWorkbook wwb = Workbook.createWorkbook(file);
			// 生成名为第一页的工作表,参数0表示这是第一页
			WritableSheet sheet = wwb.createSheet("第一页", 0);
			TaskDAO dao = new TaskDAO();
			List<Task> list = dao.list();
			List<String> sqlfield = dao.getMetaData("IGP_CONF_TASK");

			// 声明一个保存数字的单元格,使用Number的完整包路径,写全包名,防止与java.lang包的Number发生冲突
			jxl.write.Number TASK_ID;
			Label TASK_DESCRIBE;
			jxl.write.Number DEV_ID;
			jxl.write.Number DEV_PORT;
			jxl.write.Number PROXY_DEV_ID;
			jxl.write.Number PROXY_DEV_PORT;
			jxl.write.Number COLLECT_TYPE;
			jxl.write.Number COLLECT_PERIOD;
			jxl.write.Number COLLECTTIMEOUT;
			jxl.write.Number COLLECT_TIME;
			Label COLLECT_PATH; // 大字段 clob
			jxl.write.Number SHELL_TIMEOUT;
			jxl.write.Number PARSE_TMPID;
			jxl.write.Number DISTRBUTE_TMPID;
			Label SUC_DATA_TIME; // 日期
			jxl.write.Number SUC_DATA_POS;
			jxl.write.Number ISUSED;
			jxl.write.Number ISUPDATE;
			jxl.write.Number MAXCLTTIME;
			Label SHELL_CMD_PREPARE;
			Label SHELL_CMD_FINISH;
			jxl.write.Number COLLECT_TIMEPOS;
			Label DBDRIVER;
			Label DBURL;
			jxl.write.Number THREADSLEEPTIME;
			jxl.write.Number BLOCKEDTIME;
			Label COLLECTOR_NAME;
			jxl.write.Number PARAMRECORD;
			jxl.write.Number GROUP_ID;
			Label END_DATA_TIME; // 日期
			jxl.write.Number PARSERID;
			jxl.write.Number DISTRIBUTORID;
			jxl.write.Number REDO_TIME_OFFSET;

			Label tablefield;
			// i与j用于计数
			int i = 0;
			int j = 0;
			// 遍历集合中的元素
			Iterator<Task> it = list.iterator();
			Iterator<String> itfield = sqlfield.iterator();
			for (int num = 0; num <= list.size(); num++) {

				// String field ="";
				if (j == 0) {
					// 添加表字段
					for (int n = 0; n < sqlfield.size(); n++) {
						tablefield = new Label(n, j, itfield.next());
						sheet.addCell(tablefield);
					}
				} else {
					Task t = it.next();
					TASK_ID = new jxl.write.Number(i++, j, t.getTaskId()); // 第一列,第一行
					TASK_DESCRIBE = new Label(i++, j, t.getTaskDescribe()); // 将第二列,第一行

					DEV_ID = new jxl.write.Number(i++, j, t.getDevId()); // 将第三列,第一行
					DEV_PORT = new jxl.write.Number(i++, j, t.getDevPort()); // 将第四列,第一行
					PROXY_DEV_ID = new jxl.write.Number(i++, j, t.getProxyDevId()); // 将第五列,第一行
					PROXY_DEV_PORT = new jxl.write.Number(i++, j, t.getProxyDevPort());
					COLLECT_TYPE = new jxl.write.Number(i++, j, t.getCollectType().getValue());
					COLLECT_PERIOD = new jxl.write.Number(i++, j, t.getCollectPeriod().getValue());
					COLLECTTIMEOUT = new jxl.write.Number(i++, j, t.getCollectTimeout());
					COLLECT_TIME = new jxl.write.Number(i++, j, t.getCollectTime());
					COLLECT_PATH = new Label(i++, j, t.getCollectPath()); // clob
					SHELL_TIMEOUT = new jxl.write.Number(i++, j, t.getShellTimeout());
					PARSE_TMPID = new jxl.write.Number(i++, j, t.getParserId());
					DISTRBUTE_TMPID = new jxl.write.Number(i++, j, t.getDistributeTmpId());
					if (t.getSucDataTime() != null) {
						SUC_DATA_TIME = new Label(i++, j, t.getSucDataTime().toString()); // date
					} else {
						SUC_DATA_TIME = new Label(i++, j, ""); // date
					}
					SUC_DATA_POS = new jxl.write.Number(i++, j, t.getSucDataPos());
					ISUSED = new jxl.write.Number(i++, j, t.getIsUsed());
					ISUPDATE = new jxl.write.Number(i++, j, t.getIsUpdate());
					MAXCLTTIME = new jxl.write.Number(i++, j, t.getMaxCltTime());

					SHELL_CMD_PREPARE = new Label(i++, j, t.getShellCmdPrepare());
					SHELL_CMD_FINISH = new Label(i++, j, t.getShellCmdFinish());

					COLLECT_TIMEPOS = new jxl.write.Number(i++, j, t.getCollectTimepos());

					DBDRIVER = new Label(i++, j, t.getDbDriver());
					DBURL = new Label(i++, j, t.getDbUrl());

					THREADSLEEPTIME = new jxl.write.Number(i++, j, t.getThreadSleepTime());
					BLOCKEDTIME = new jxl.write.Number(i++, j, t.getBlockTime());
					COLLECTOR_NAME = new Label(i++, j, t.getCollectorName());
					PARAMRECORD = new jxl.write.Number(i++, j, t.getParamRecord());
					GROUP_ID = new jxl.write.Number(i++, j, t.getGroupId());

					// END_DATA_TIME = new Label(i++, j,
					// t.getEndDataTime().toString());
					if (t.getEndDataTime() != null) {
						END_DATA_TIME = new Label(i++, j, t.getEndDataTime().toString());
					} else {
						END_DATA_TIME = new Label(i++, j, "");
					}

					PARSERID = new jxl.write.Number(i++, j, t.getParserId());
					DISTRIBUTORID = new jxl.write.Number(i++, j, t.getDistributorId());
					REDO_TIME_OFFSET = new jxl.write.Number(i++, j, t.getRedoTimeOffset());

					// 将定义好的单元格添加到工作表中
					sheet.addCell(TASK_ID);
					sheet.addCell(TASK_DESCRIBE);
					sheet.addCell(DEV_ID);
					sheet.addCell(DEV_PORT);
					sheet.addCell(PROXY_DEV_ID);
					sheet.addCell(PROXY_DEV_PORT);
					sheet.addCell(COLLECT_TYPE);
					sheet.addCell(COLLECT_PERIOD);
					sheet.addCell(COLLECTTIMEOUT);
					sheet.addCell(COLLECT_TIME);
					sheet.addCell(COLLECT_PATH);
					sheet.addCell(SHELL_TIMEOUT);
					sheet.addCell(PARSE_TMPID);
					sheet.addCell(DISTRBUTE_TMPID);
					sheet.addCell(SUC_DATA_TIME);
					sheet.addCell(SUC_DATA_POS);
					sheet.addCell(ISUSED);
					sheet.addCell(ISUPDATE);
					sheet.addCell(MAXCLTTIME);
					sheet.addCell(SHELL_CMD_PREPARE);
					sheet.addCell(SHELL_CMD_FINISH);
					sheet.addCell(COLLECT_TIMEPOS);
					sheet.addCell(DBDRIVER);
					sheet.addCell(DBURL);
					sheet.addCell(THREADSLEEPTIME);
					sheet.addCell(BLOCKEDTIME);
					sheet.addCell(COLLECTOR_NAME);
					sheet.addCell(PARAMRECORD);
					sheet.addCell(GROUP_ID);
					sheet.addCell(END_DATA_TIME);
					sheet.addCell(PARSERID);
					sheet.addCell(DISTRIBUTORID);
					sheet.addCell(REDO_TIME_OFFSET);
				}
				/** * i = 0; 那么下次循环后开始还是第一列* j++; 那么下次循环后开始就是第二行 */
				i = 0;
				j++;

			}
			// 写入数据并关闭文件
			wwb.write();
			wwb.close();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (RowsExceededException e) {
			e.printStackTrace();
		} catch (WriteException e) {
			e.printStackTrace();
		}

	}

	public List<String> getTemPFileName() {
		String tempName = null;

		String sql = "select tmp.tempfilename from IGP_CONF_TASK t, igp_conf_templet tmp where (t.parse_tmpid = tmp.tmpid or t.distrbute_tmpid = tmp.tmpid) and t.isused =1";
		List<String> tempNames = new ArrayList<String>();
		Connection con = DbPool.getConn();
		Statement st = null;
		ResultSet rs = null;
		try {
			st = con.createStatement();
			rs = st.executeQuery(sql);
			while (rs.next()) {

				tempName = rs.getString("TEMPFILENAME");
				tempNames.add(tempName);
			}
		} catch (Exception e) {
			logger.error("查询记录时异常：" + sql, e);
		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
				if (st != null) {
					st.close();
				}
				if (con != null) {
					con.close();
				}
			} catch (Exception e) {
			}
		}
		return tempNames;
	}

	private static boolean deleteFile(File file) {
		if (!file.exists())
			return false;
		if (!file.isDirectory())
			return false;
		File[] files = file.listFiles();
		for (File deleteFile : files) {
			if (deleteFile.isDirectory()) {
				// 如果是文件夹，则递归删除下面的文件后再删除该文件夹
				if (!deleteFile(deleteFile)) {
					// 如果失败则返回
					return false;
				}
			} else {
				if (!deleteFile.delete()) {
					// 如果失败则返回
					return false;
				}
			}
		}
		return file.delete();

	}

	public static void main(String[] args) throws IOException {

		// /BugServlet bug = new BugServlet();
		// bug.createTemExl();
		// bug.createTaskExl();
		/*
		 * List<String> list = bug.getTemPFileName(); for(String s : list){ System.out.println(s); }
		 */
		/*
		 * String downloadPaht="c:\\uwaydownload"; File f = new File(downloadPaht); bug.deleteFile(f); File zip = new File("c:\\uwayCollectLog.zip");
		 * zip.delete();
		 */

	}

}
