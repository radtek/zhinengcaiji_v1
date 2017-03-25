package web.servlet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import db.dao.CollectLogDAO;
import db.pojo.ActionResult;

public class CollectLogServlet extends BasicServlet<CollectLogDAO> {

	private static final long serialVersionUID = 1L;

	private String sql = "select * from igp_data_log";

	private final int pageSize = 10;

	/**
	 * 查询采集日志记录
	 */
	@Override
	public ActionResult query(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		ActionResult result = new ActionResult();
		result.setForwardURL("collectLog.jsp");
		int currentPage = 1;
		result.setData(dao.pageQuery(pageSize, currentPage, sql));
		return result;
	}

	public ActionResult queryPage(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		ActionResult result = new ActionResult();
		result.setForwardURL("collectLog.jsp");
		String type = req.getParameter("id");
		if (req.getSession().getAttribute("selectSql") != null) {
			sql = (String) req.getSession().getAttribute("selectSql");
		}
		if (type.equals("firstPage")) {
			result = query(req, resp);

		} else if (type.equals("lastPage")) {
			int recordCount = dao.pageCount(sql);
			int pageCount = dao.getPageCount(pageSize, recordCount);
			result.setData(dao.pageQuery(pageSize, pageCount, sql));

		} else if (type.equals("prePage")) {
			String num = req.getParameter("pageNum");
			int intNum = Integer.parseInt(num);
			int currentPage = intNum - 1;
			if (currentPage == 0) {
				currentPage = 1;
			}
			result.setData(dao.pageQuery(pageSize, currentPage, sql));

		} else if (type.equals("nextPage")) {
			String num = req.getParameter("pageNum");
			int intNum = Integer.parseInt(num);
			int currentPage = intNum + 1;
			int recordCount = dao.pageCount(sql);
			int pageCount = dao.getPageCount(pageSize, recordCount);
			if (currentPage > pageCount) {
				currentPage = pageCount;
			}
			result.setData(dao.pageQuery(pageSize, currentPage, sql));

		} else if (type.equals("inputPage")) {
			result.setForwardURL("collectLog.jsp");
			String goValue = req.getParameter("go");
			int currentPage = Integer.parseInt(goValue);
			int recordCount = dao.pageCount(sql);
			int pageCount = dao.getPageCount(pageSize, recordCount);
			if (currentPage > pageCount) {
				currentPage = pageCount;
			}
			result.setData(dao.pageQuery(pageSize, currentPage, sql));
		}
		return result;
	}

	public ActionResult selectLog(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		ActionResult result = new ActionResult();
		result.setForwardURL("collectLog.jsp");
		String taskId = req.getParameter("taskId");
		String stampStartTime = req.getParameter("stampStartTime");
		String stampEndTime = req.getParameter("stampEndTime");
		String taskType = req.getParameter("taskType");
		String taskResult = req.getParameter("taskResult");
		String taskException = req.getParameter("taskException");
		String basicSQL = "select t.* from igp_data_log t";
		StringBuffer sql = new StringBuffer(basicSQL);
		// 存放所有的条件
		List<String> conditions = new ArrayList<String>();
		int idNum = 0;
		if (!"".equals(taskId)) {
			idNum = Integer.parseInt(taskId);
		}
		if (idNum > 0) {
			conditions.add("t.task_id=" + idNum);
		}
		if (!"".equals(taskType)) {
			if (taskType.equals("task")) {
				conditions.add("t.TASK_TYPE=" + " '正常任务'");
			} else {
				conditions.add("t.TASK_TYPE=" + " '补采任务'");
			}
		}
		if (!"".equals(taskResult)) {
			if (taskResult.equals("success")) {
				conditions.add("t.TASK_RESULT=" + "'成功'");
			} else if (taskResult.equals("partSuccess")) {
				conditions.add("t.TASK_RESULT=" + "'部分成功'");
			} else if (taskResult.equals("fail")) {
				conditions.add("t.TASK_RESULT=" + "'失败'");
			}
		}
		if (!"".equals(taskException)) {
			if (taskException.equals("yes")) {
				conditions.add("t.TASK_EXCEPTION" + " is not null");
			} else {
				conditions.add("t.TASK_EXCEPTION" + " is null");
			}
		}
		if (!"".equals(stampStartTime) && !"".equals(stampEndTime)) {
			conditions.add("t.DATA_TIME   between to_date('" + stampStartTime + "','yyyy-mm-dd HH24:mi:ss') and " + "to_date('" + stampEndTime
					+ "','yyyy-mm-dd HH24:mi:ss')");
		} else if (!"".equals(stampStartTime)) {
			conditions.add("t.DATA_TIME >= to_date('" + stampStartTime + "','yyyy-mm-dd HH24:mi:ss')");
		} else if (!"".equals(stampEndTime)) {
			conditions.add("t.DATA_TIME <= to_date('" + stampEndTime + "','yyyy-mm-dd HH24:mi:ss')");
		}
		// 如果有条件
		if (conditions.size() >= 1) {
			sql.append(" where ").append(conditions.get(0));
			for (int i = 1; i < conditions.size(); i++) {
				sql.append(" and ").append(conditions.get(i));
			}
		}

		HttpSession session = req.getSession();
		session.setAttribute("selectSql", sql.toString());
		result.setData(dao.selectLog(sql.toString(), pageSize, 1));

		return result;

	}

}
