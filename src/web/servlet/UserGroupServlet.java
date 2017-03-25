package web.servlet;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import db.dao.UserGroupDAO;
import db.pojo.ActionResult;
import db.pojo.UserGroup;
import framework.IGPError;

public class UserGroupServlet extends BasicServlet<UserGroupDAO> {

	private static final long serialVersionUID = 1L;

	public ActionResult query(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		ActionResult result = new ActionResult();
		result.setForwardURL("usergroup.jsp");
		result.setData(dao.list());
		return result;

	}

	public ActionResult add(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		String forwardURL = req.getParameter("forwardURL");
		String returnURL = req.getParameter("returnURL");
		ActionResult result = new ActionResult();

		String groupId = req.getParameter("groupId");
		String groupName = req.getParameter("groupName");
		String ids = req.getParameter("ids");
		String note = req.getParameter("note");

		UserGroup group = new UserGroup();
		group.setId(Integer.parseInt(groupId));
		group.setName(groupName);
		group.setIds(ids);
		group.setNote(note);
		boolean bool = dao.validate(group);
		if (bool) {
			int num = dao.add(group);
			if (num >= 1) {
				result.setError(new IGPError("", "ok", "", ""));
			} else {
				result.setError(new IGPError("", "失败", "", ""));
			}
		} else {
			result.setError(new IGPError("", "分组编号已经存在", "", ""));
		}
		result.setForwardURL(forwardURL);
		result.setReturnURL(returnURL);
		return result;

	}

	public ActionResult userAdd(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		ActionResult result = new ActionResult();
		result.setForwardURL("userAdd.jsp");
		result.setData(dao.list());
		return result;
	}

	public ActionResult update(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		ActionResult result = new ActionResult();
		String strID = req.getParameter("id");
		result.setForwardURL("usergroupUpdate.jsp");
		result.setData(dao.getById(Integer.parseInt(strID)));
		// req.setAttribute("groups", dao.list());
		return result;
	}

	public ActionResult updateResult(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		String forwardURL = req.getParameter("forwardURL");
		String returnURL = req.getParameter("returnURL");
		ActionResult result = new ActionResult();
		String groupId = req.getParameter("groupId");
		String groupName = req.getParameter("groupName");
		String ids = req.getParameter("ids");
		String note = req.getParameter("note");
		UserGroup group = new UserGroup();
		group.setId(Integer.parseInt(groupId));
		group.setName(groupName);
		group.setIds(ids);
		group.setNote(note);
		result.setForwardURL(forwardURL);
		result.setReturnURL(returnURL);
		result.setData(dao.update(group));;
		return result;
	}

}
