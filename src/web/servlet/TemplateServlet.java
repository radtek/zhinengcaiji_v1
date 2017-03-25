package web.servlet;

import java.io.File;
import java.io.IOException;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import util.Util;
import db.dao.TempletDAO;
import db.pojo.ActionResult;
import db.pojo.Templet;
import framework.IGPError;
import framework.SystemConfig;

/**
 * 模板管理
 * 
 * @author yuanxf
 * @since 1.0
 */
public class TemplateServlet extends BasicServlet<TempletDAO> {

	private static final long serialVersionUID = 1L;

	/**
	 * 查询模板
	 */
	public ActionResult query(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		ActionResult result = new ActionResult();
		result.setForwardURL("templet.jsp");

		String type = req.getParameter("type");
		String keyword = req.getParameter("keyword");

		// 添加附加参数,以备前台页面使用
		result.setWparam(keyword);
		result.setLparam(type);

		// 条件查询
		result.setData(queryByCondition(keyword, type));

		return result;
	}

	/**
	 * 修改模板信息
	 */
	public ActionResult update(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		ActionResult result = new ActionResult();
		result.setForwardURL(DEFAULT_FORWARD_URL);
		result.setReturnURL(DEFAULT_RETURN_URL);

		// 获取Request中表单的数据
		String strTmpID = req.getParameter("id");
		String strTmpType = req.getParameter("tmpType");
		String tmpName = req.getParameter("tmpName");
		String edition = req.getParameter("edition");
		String tempFileName = req.getParameter("tempFileName");

		// 校验数据
		int tmpID = -1;
		int tmpType = -1;
		try {
			tmpID = Integer.parseInt(strTmpID);
			tmpType = Integer.parseInt(strTmpType);
		} catch (NumberFormatException e) {
		}

		if (tmpID < 0 || tmpType < 0) {
			result.setError(new IGPError());
			return result;
		}

		// 查看此记录是否存在并且比较传入的模板文件名和数据库原始记录的模板文件名是否相同
		Templet tTemp = dao.getById(tmpID);
		if (tTemp == null) {
			result.setError(new IGPError());
			return result;
		} else {
			/*
			 * String oldFileName = tTemp.getTempFileName(); if ( !tempFileName.equals(oldFileName) ) { if ( Util.isNotNull(tempFileName) ) { //
			 * 检查添加的模板文件名在系统模板路径下是否已经存在 if ( templetFileExists(tempFileName) ) { result.setError(new IGPError()); return result; } } }
			 */
		}

		// 构建Templet对象
		Templet temp = new Templet();
		temp.setTmpID(tmpID);
		temp.setTmpType(tmpType);
		temp.setTmpName(tmpName);
		temp.setEdition(edition);
		temp.setTempFileName(tempFileName);

		boolean b = dao.update(temp);
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
	 * 添加模板
	 */
	public ActionResult add(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		ActionResult result = new ActionResult();
		result.setForwardURL(DEFAULT_FORWARD_URL);
		result.setReturnURL(DEFAULT_RETURN_URL);

		// 获取Request中表单的数据
		String strTmpID = req.getParameter("tmpID");
		String strTmpType = req.getParameter("tmpType");
		String tmpName = req.getParameter("tmpName");
		String edition = req.getParameter("edition");
		String tempFileName = req.getParameter("tempFileName");

		// 校验数据
		int tmpID = -1;
		int tmpType = -1;
		try {
			tmpID = Integer.parseInt(strTmpID);
			tmpType = Integer.parseInt(strTmpType);
		} catch (NumberFormatException e) {
		}

		if (tmpID < 0 || tmpType < 0) {
			result.setError(new IGPError());
			return result;
		}

		/*
		 * if ( Util.isNotNull(tempFileName) ) { // 检查添加的模板文件名在系统模板路径下是否已经存在 if ( templetFileExists(tempFileName) ) { result.setError(new IGPError());
		 * return result; } }
		 */

		// 构建Templet对象
		Templet temp = new Templet();
		temp.setTmpID(tmpID);
		temp.setTmpType(tmpType);
		temp.setTmpName(tmpName);
		temp.setEdition(edition);
		temp.setTempFileName(tempFileName);

		/*
		 * 添加模板必须满足以下约束 1.模板编号不能重复; 2.模板文件名不能重复;
		 */
		if (dao.exists(temp)) {
			result.setError(new IGPError());
		} else {
			int num = dao.add(temp);
			if (num == 1) {
				// 数据添加成功
				result.setError(new IGPError());
				result.setReturnURL("templetAdd.jsp");
			} else {
				// 数据添加失败
				result.setError(new IGPError());
			}
		}

		return result;
	}

	/**
	 * 条件查询
	 * 
	 * @param keyword
	 *            关键字
	 * @param type
	 *            属性类型
	 * @return
	 */
	private List<Templet> queryByCondition(String keyword, String type) {
		Templet temp = new Templet();

		if (Util.isNotNull(type) && Util.isNotNull(keyword)) {
			if (type.equalsIgnoreCase("编号")) {
				temp.setTmpID(Integer.parseInt(keyword));
			} else if (type.equalsIgnoreCase("类型")) {
				temp.setTmpType(Integer.parseInt(keyword));
			} else if (type.equalsIgnoreCase("文件名")) {
				temp.setTempFileName(keyword);
			} else if (type.equalsIgnoreCase("描述")) {
				temp.setTmpName(keyword);
			}
		}

		return dao.criteriaQuery(temp);
	}

	/**
	 * 检查模板文件名在系统模板路径下是否已经存在
	 * 
	 * @param fileName
	 *            模板文件名，不包含路径
	 * @return
	 */
	public boolean templetFileExists(String fileName) {
		String cFileName = SystemConfig.getInstance().getTempletPath() + File.separator + fileName;
		File f = new File(cFileName);
		if (f.exists() && f.isFile()) {
			return true;
		}

		return false;
	}
}
