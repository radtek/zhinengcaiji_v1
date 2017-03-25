package web.servlet;

import java.io.IOException;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import util.Util;
import db.dao.DeviceDAO;
import db.pojo.ActionResult;
import db.pojo.Device;
import framework.IGPError;

/**
 * 设备管理
 * 
 * @author yuanxf
 * @since 1.0
 */
public class DeviceServlet extends BasicServlet<DeviceDAO> {

	private static final long serialVersionUID = 2783114018914741489L;

	/**
	 * 查询设备
	 */
	@Override
	public ActionResult query(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		ActionResult result = new ActionResult();
		result.setForwardURL("device.jsp");

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
	 * 条件查询
	 * 
	 * @param keyword
	 *            关键字
	 * @param type
	 *            属性类型
	 * @return
	 */
	private List<Device> queryByCondition(String keyword, String type) {
		Device dev = new Device();

		if (Util.isNotNull(type) && Util.isNotNull(keyword)) {
			if (type.equalsIgnoreCase("id")) {
				dev.setDevID(Integer.parseInt(keyword));
			} else if (type.equalsIgnoreCase("name")) {
				dev.setDevName(keyword);
			} else if (type.equalsIgnoreCase("omcid")) {
				dev.setOmcID(Integer.parseInt(keyword));
			}
		}

		return dao.criteriaQuery(dev);
	}

	/**
	 * 添加设备
	 */
	@Override
	public ActionResult add(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		ActionResult result = new ActionResult();
		result.setForwardURL(DEFAULT_FORWARD_URL);
		result.setReturnURL(DEFAULT_RETURN_URL);

		// 获取Request中表单的数据
		String strDeviceId = req.getParameter("deviceId");
		String deviceName = req.getParameter("deviceName");
		String strCityId = req.getParameter("cityId");
		String strOmcId = req.getParameter("omcId");
		String vendor = req.getParameter("vendor");
		String hostIp = req.getParameter("hostIp");
		String userName = req.getParameter("userName");
		String password = req.getParameter("password");
		String hostSign = req.getParameter("hostSign");

		// 校验数据
		int deviceId = -1;
		int cityId = -1;
		int omcId = -1;
		try {
			deviceId = Integer.parseInt(strDeviceId);
			cityId = Integer.parseInt(strCityId);
			omcId = Integer.parseInt(strOmcId);
		} catch (NumberFormatException e) {
		}

		if (deviceId < 0 || cityId < 0 || omcId < 0 || Util.isNull(deviceName) || Util.isNull(hostIp)) {
			result.setError(new IGPError());
			return result;
		}

		// 构建Device对象
		Device dev = new Device();
		dev.setDevID(deviceId);
		dev.setDevName(deviceName);
		dev.setCityID(cityId);
		dev.setOmcID(omcId);
		dev.setVendor(vendor);
		dev.setHostIP(hostIp);
		dev.setHostUser(userName);
		dev.setHostPwd(password);
		dev.setHostSign(hostSign);

		/*
		 * 添加设备必须满足以下约束 1.设备编号不能重复; 2.不能出现城市编号、OMCID、IP三者同时相等的记录;
		 */

		boolean b = dao.exists(dev);
		if (b) {
			result.setError(new IGPError());
		}
		// 不存在相同记录则添加
		else {
			int num = dao.add(dev);
			if (num == 1) {
				// 数据添加成功
				result.setError(new IGPError());
				result.setReturnURL("deviceAdd.jsp");
			} else {
				// 数据添加失败
				result.setError(new IGPError());
			}
		}

		return result;
	}

	/**
	 * 修改设备信息
	 */
	@Override
	public ActionResult update(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		ActionResult result = new ActionResult();
		result.setForwardURL(DEFAULT_FORWARD_URL);
		result.setReturnURL(DEFAULT_RETURN_URL);

		// 获取Request中表单的数据
		String strDeviceId = req.getParameter("id");
		String deviceName = req.getParameter("deviceName");
		String strCityId = req.getParameter("cityId");
		String strOmcId = req.getParameter("omcId");
		String vendor = req.getParameter("vendor");
		String hostIp = req.getParameter("hostIp");
		String userName = req.getParameter("userName");
		String password = req.getParameter("password");
		String hostSign = req.getParameter("hostSign");

		// 校验数据
		int deviceId = -1;
		int cityId = -1;
		int omcId = -1;
		try {
			deviceId = Integer.parseInt(strDeviceId);
			cityId = Integer.parseInt(strCityId);
			omcId = Integer.parseInt(strOmcId);
		} catch (NumberFormatException e) {
		}

		if (deviceId < 0 || cityId < 0 || omcId < 0 || Util.isNull(deviceName) || Util.isNull(hostIp)) {
			result.setError(new IGPError());
			return result;
		}

		// 构建Device对象
		Device dev = new Device();
		dev.setDevID(deviceId);
		dev.setDevName(deviceName);
		dev.setCityID(cityId);
		dev.setOmcID(omcId);
		dev.setVendor(vendor);
		dev.setHostIP(hostIp);
		dev.setHostUser(userName);
		dev.setHostPwd(password);
		dev.setHostSign(hostSign);

		boolean b = dao.update(dev);
		if (b) {
			// 数据更新成功
			result.setError(new IGPError());
		} else {
			// 数据更新失败
			result.setError(new IGPError());
		}

		return result;
	}

}
