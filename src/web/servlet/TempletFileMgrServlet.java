package web.servlet;

import java.io.IOException;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import util.Util;
import db.dao.TempletFileDAO;
import db.pojo.ActionResult;
import db.pojo.TempletFile;
import framework.IGPError;
import framework.SystemConfig;

/**
 * 模板文件操作Servlet
 * 
 * @author YangJian
 * @since 1.0
 */
public class TempletFileMgrServlet extends BasicServlet<TempletFileDAO> {

	private static final long serialVersionUID = -7689907793409425249L;

	/**
	 * 查询设备
	 */
	@Override
	public ActionResult query(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		ActionResult result = new ActionResult();
		result.setForwardURL("templetFileMgr.jsp");

		String keyword = req.getParameter("keyword");

		// 添加附加参数,以备前台页面使用
		result.setWparam(keyword);
		// 设定机器名及系统模板路径
		result.setLparam(Util.getHostName() + " : " + SystemConfig.getInstance().getTempletPath());

		// 条件查询
		result.setData(queryByCondition(keyword));

		return result;
	}

	/**
	 * 条件查询
	 * 
	 * @param keyword
	 *            关键字,即文件名
	 * @return
	 */
	private List<TempletFile> queryByCondition(String keyword) {
		TempletFile t = new TempletFile();

		if (Util.isNotNull(keyword)) {
			t.setName(keyword);
		}

		return dao.criteriaQuery(t);
	}

	/**
	 * 删除采集机上的模板文件
	 */
	@Override
	public ActionResult del(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		ActionResult result = new ActionResult();
		String strFileName = req.getParameter("id"); // 文件名
		String forwardURL = req.getParameter("forwardURL");
		String returnURL = req.getParameter("returnURL");

		// 如果不填写forwardURL则为默认页面
		if (Util.isNull(forwardURL))
			forwardURL = DEFAULT_FORWARD_URL;

		if (Util.isNull(returnURL) || Util.isNull(strFileName)) {
			result.setError(new IGPError());
			result.setData(null);
			result.setReturnURL(DEFAULT_RETURN_URL);
		} else {
			TempletFile tf = new TempletFile();
			tf.setName(strFileName);

			// 删除成功
			if (dao.delete(tf)) {
				result.setError(new IGPError());
			}
			// 删除失败
			else {
				result.setError(new IGPError());
			}
		}
		result.setReturnURL(returnURL);
		result.setForwardURL(forwardURL);

		return result;
	}

	/**
	 * 读取指定名称的模板文件信息
	 */
	@Override
	public ActionResult get(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		ActionResult result = new ActionResult();
		String strID = req.getParameter("id");
		String forwardURL = req.getParameter("forwardURL");
		String returnURL = req.getParameter("returnURL");

		if (Util.isNull(strID) || Util.isNull(forwardURL) || Util.isNull(returnURL)) {
			result.setError(new IGPError());
			result.setData(null);
			result.setForwardURL(DEFAULT_FORWARD_URL);
			result.setReturnURL(DEFAULT_RETURN_URL);
		} else {
			result.setError(new IGPError());
			result.setData(dao.getByName(strID));
			result.setForwardURL(forwardURL);
			result.setReturnURL(returnURL);
		}

		// wparam 保存文件是否超过大小判断值
		result.setWparam(dao.isExceedMaxValue(strID));

		return result;
	}

	public ActionResult getContentByAjax(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		String name = req.getParameter("name");
		resp.setContentType("text/plain;charset=utf-8");
		String content = dao.getByName(name).getContent();
		resp.getWriter().println(content);
		return new ActionResult(new IGPError(), null, "templetFileMgr.jsp", content);
	}

	/**
	 * 保存模板内容到文件
	 */
	@Override
	public ActionResult update(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		ActionResult result = new ActionResult();
		result.setForwardURL(DEFAULT_FORWARD_URL);
		result.setReturnURL(DEFAULT_RETURN_URL);

		// 获取Request中表单的数据
		String fileName = req.getParameter("id");
		String content = req.getParameter("content");

		// 校验数据
		if (Util.isNull(fileName)) {
			result.setError(new IGPError());
			return result;
		}

		// 构建TempletFile对象
		TempletFile tf = new TempletFile();
		tf.setName(fileName);
		tf.setContent(content);

		// 更新文件内容
		boolean b = dao.update(tf);
		if (b) {
			// 数据更新成功
			result.setError(new IGPError());
		} else {
			// 数据更新失败
			result.setError(new IGPError());
		}

		return result;
	}

	/**
	 * 重命名模板文件
	 */
	public ActionResult renameAjax(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		ActionResult result = new ActionResult();
		result.setForwardURL(null);
		result.setReturnURL(null);

		// 获取Request中表单的数据
		String fileName = req.getParameter("id");
		String newFileName = req.getParameter("newName");

		// 校验数据
		if (Util.isNull(fileName) || Util.isNull(newFileName)) {
			resp.getWriter().println("操作失败.原因:传入参数有误.");
			return result;
		}

		// 重命名文件
		boolean b = dao.rename(fileName, newFileName);
		if (b) {
			resp.getWriter().println("操作成功");
		} else {
			resp.getWriter().println("操作失败");
		}

		return result;
	}

	/**
	 * 新建模板文件
	 */
	public ActionResult newFileAjax(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		ActionResult result = new ActionResult();
		result.setForwardURL(null);
		result.setReturnURL(null);

		// 获取Request中表单的数据
		String fileName = req.getParameter("id");

		// 校验数据
		if (Util.isNull(fileName)) {
			resp.getWriter().println("操作失败.原因:传入参数有误.");
			return result;
		}

		// 新建文件
		boolean b = dao.newFile(fileName);
		if (b) {
			resp.getWriter().println("操作成功");
		} else {
			resp.getWriter().println("操作失败");
		}

		return result;
	}

}
