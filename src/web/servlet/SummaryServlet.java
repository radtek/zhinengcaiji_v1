package web.servlet;

import java.io.IOException;
import java.text.ParseException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import util.LogMgr;
import util.Util;
import db.dao.PageQueryResult;
import db.dao.SummaryDAO;
import db.pojo.ActionResult;
import db.pojo.LogCltInsert;
import framework.IGPError;

public class SummaryServlet extends BasicServlet<SummaryDAO> {

	private Logger logger = LogMgr.getInstance().getSystemLogger();

	private static final long serialVersionUID = -5617339395955701086L;

	private static final int PAGE_SIZE = 10;

	@Override
	public ActionResult query(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		int currentPage = Util.isNull(req.getParameter("currentPage")) ? 1 : Integer.parseInt(req.getParameter("currentPage"));
		LogCltInsert pojo = req.getParameter("adv") != null ? buildPOJO(req) : null;
		if (pojo != null) {
			req.getSession().setAttribute("condition", pojo);
		}

		if (pojo == null && req.getSession().getAttribute("condition") != null) {
			pojo = (LogCltInsert) req.getSession().getAttribute("condition");
		}
		PageQueryResult<LogCltInsert> datas = dao.pageQuery(pojo, PAGE_SIZE, currentPage);
		if (pojo == null) {
			req.getSession().removeAttribute("condition");
		}
		return new ActionResult(new IGPError(), "/page/sysmonitor/summaryInterface.jsp", "/page/sysmonitor/summaryInterface.jsp", datas);
	}

	private LogCltInsert buildPOJO(HttpServletRequest req) {
		LogCltInsert lci = new LogCltInsert();
		lci.setCalFlag((byte) (Util.isNull(req.getParameter("calFlag")) ? -1 : Integer.parseInt(req.getParameter("calFlag"))));
		lci.setCount(Util.isNull(req.getParameter("count")) ? -1 : Integer.parseInt(req.getParameter("count")));
		lci.setOmcID(Util.isNull(req.getParameter("omcid")) ? -1 : Integer.parseInt(req.getParameter("omcid")));
		try {
			lci.setStampTime(req.getParameter("stamptime") == null ? null : Util.getDate1(req.getParameter("stamptime")));
			lci.setVSysDate(req.getParameter("sysDate") == null ? null : Util.getDate1(req.getParameter("sysDate")));
		} catch (ParseException e) {
			logger.error("日期转换异常", e);
		}
		lci.setTaskID(Util.isNull(req.getParameter("taskId")) ? -1 : Integer.parseInt(req.getParameter("taskId")));
		lci.setTbName(req.getParameter("tbName"));
		return lci;
	}
}
