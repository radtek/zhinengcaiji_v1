package web.servlet;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URLEncoder;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import util.Util;
import db.dao.TempletFileDAO;

/**
 * 下载模板文件Servlet
 * 
 * @author YangJian
 * @since 1.0
 */
public class TempletFileDownloadServlet extends HttpServlet {

	private static final long serialVersionUID = 3585943825212125464L;

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		this.doPost(req, resp);
	}

	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		// 获取请求下载的文件名
		String fileName = req.getParameter("id");
		if (Util.isNull(fileName))
			return;

		TempletFileDAO dao = new TempletFileDAO();
		fileName = fileName.trim();

		resp.setContentType("application/x-download");
		// resp.setContentType("application/force-download");
		String fileDownload = dao.getTempletFolderPath() + File.separator + fileName;
		String fileDisplay = fileName; // 给用户提供的下载文件名
		fileDisplay = URLEncoder.encode(fileDisplay, "UTF-8");
		resp.addHeader("Content-Disposition", "attachment;filename=" + fileDisplay);

		java.io.OutputStream outp = null;
		java.io.FileInputStream in = null;
		try {
			outp = resp.getOutputStream();
			in = new FileInputStream(fileDownload);
			byte[] b = new byte[1024];
			int i = 0;
			while ((i = in.read(b)) > 0) {
				outp.write(b, 0, i);
			}
			outp.flush();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (in != null) {
				in.close();
				in = null;
			}
			if (outp != null) {
				outp.close();
				outp = null;
			}
		}
	}

}
