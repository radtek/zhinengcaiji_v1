package web.servlet;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.fileupload.FileItemIterator;
import org.apache.commons.fileupload.FileItemStream;
import org.apache.commons.fileupload.FileUploadBase.FileSizeLimitExceededException;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.commons.fileupload.util.Streams;

import db.dao.TempletFileDAO;

/**
 * 模板文件上传Servlet
 * 
 * @author YangJian
 * @since 1.0
 */
public class TempletFileUploadServlet extends HttpServlet {

	private static final long serialVersionUID = 7106471284319770048L;

	private static final long MAX_ALLOW_FILE_SIZE = 5000000;

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		this.doPost(req, resp);
	}

	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		resp.setContentType("text/html;charset=utf-8");

		TempletFileDAO dao = new TempletFileDAO();
		File tmpDir = new File("." + File.separator + "temp" + File.separator);
		String saveDir = dao.getTempletFolderPath() + File.separator;

		resp.getWriter().println("<br><br><p>");
		try {
			if (ServletFileUpload.isMultipartContent(req)) {
				DiskFileItemFactory dff = new DiskFileItemFactory();
				dff.setRepository(tmpDir); // 指定上传文件的临时目录
				dff.setSizeThreshold(1024000);// 指定在内存中缓存数据大小,单位为byte

				ServletFileUpload sfu = new ServletFileUpload(dff);
				sfu.setFileSizeMax(MAX_ALLOW_FILE_SIZE);// 指定单个上传文件的最大尺寸
				sfu.setSizeMax(10000000);// 指定一次上传多个文件的总尺寸

				FileItemIterator fii = sfu.getItemIterator(req);// 解析request请求,并返回FileItemIterator集合
				while (fii.hasNext()) {
					FileItemStream fis = fii.next(); // 从集合中获得一个文件流
					if (fis.getName() == null)
						continue;
					if (fis.getName().length() <= 0)
						continue;
					if (!fis.getName().endsWith(".xml")) {
						resp.getWriter().println("<font color=red>" + fis.getName() + " 上传失败,原因:只能上传XML文件.</font><br>");
						continue;
					}
					if (!fis.isFormField() && fis.getName().length() > 0) // 过滤掉表单中非文件域
					{
						String strFileName = fis.getName();// 获得上传文件的文件名
						int pos = strFileName.lastIndexOf(File.separator);
						if (pos != -1) {
							strFileName = strFileName.substring(pos);
						}
						BufferedInputStream in = new BufferedInputStream(fis.openStream());// 获得文件输入流
						BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(new File(saveDir + strFileName)));// 获得文件输出流
						Streams.copy(in, out, true);// 开始把文件写到你指定的上传文件夹

						resp.getWriter().println(fis.getName() + " 上传成功.<br>");
					}
				}
			}
		} catch (FileSizeLimitExceededException e) {
			resp.getWriter().println("<font color=red>上传失败,原因:文件超过最大允许大小(" + MAX_ALLOW_FILE_SIZE + "M)</font><br>");
		} catch (Exception e) {
			resp.getWriter().println("<font color=red>上传失败,原因:" + e.getMessage() + "</font><br>");
		}

		resp.getWriter().println("</p>");
		resp.getWriter().println("<br><br><a href='templetFileUpload.jsp'>返回</a>");
	}
}
