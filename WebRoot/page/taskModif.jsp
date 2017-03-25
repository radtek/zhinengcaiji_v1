<%@ page language="java" contentType="text/html; charset=utf-8"
	pageEncoding="utf-8"%>
<%@taglib prefix="fmt" uri="http://java.sun.com/jsp/jstl/fmt"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
	<head>
		<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
		<title>修改任务</title>
		<link href="/css/igp.css" rel="stylesheet" type="text/css" />
		<script type="text/javascript" src="/js/jquery.js"></script>
		<script type="text/javascript" src="/js/checkTask.js"></script>
		<script type="text/javascript" src="/js/taskModif.js"></script>
		<script language="javascript" charset="gb2312" type="text/javascript"
			src="../js/DateTimeCalendar.js"></script>
	</head>
	<body>
		<form action="/page/task" method="post" id="modifForm">
			<input type="hidden" name="taskId" id="taskId"
				value="${requestScope.result.data.taskId }" />
			<input type="hidden" value="${requestScope.result.returnURL }"
				id="returnUrl" />
			<input type="hidden" name="action" id="action" value="saveModif" />
			<table width="95%" style="margin-left: 10px; margin-top: 10px;">
				<tr>
					<td height="30" colspan="3">
						<span class="currentTitle"><font color="#FF0000">${requestScope.result.data.taskId}</font>
						</span>
					</td>
				</tr>
				<tr>
					<td height="30" colspan="3"
						style="BORDER-BOTTOM: #d4d4d4 1px dashed; height: 2px;"></td>
				</tr>
				<tr>
					<td valign="top">
						<table id="detailTable">
							<tr>
								<td>
									是否启用
								</td>
								<td>
									<input type="text" name="isUsed" id="isUsed"
										value="${requestScope.result.data.isUsed }" />
								</td>
							</tr>
							<tr>
								<td>
									任务描述
								</td>
								<td>
									<input type="text" name="taskDescribe" id="taskDescribe"
										value="${requestScope.result.data.taskDescribe }" />
								</td>
							</tr>
							<tr>
								<td>
									设备号
								</td>
								<td>
									<input type="text" name="devId" id="devId"
										value="${requestScope.result.data.devId }" />
								</td>
							</tr>
							<tr>
								<td>
									设备端口
								</td>
								<td>
									<input type="text" name="devPort" id="devPort"
										value="${requestScope.result.data.devPort }" />
								</td>
							</tr>
							<tr>
								<td>
									代理设备号
								</td>
								<td>
									<input type="text" name="proxyDevId" id="proxyDevId"
										value="${requestScope.result.data.proxyDevId == -1 ? '' : requestScope.result.data.proxyDevId   }" />
								</td>
							</tr>
							<tr>
								<td>
									代理设备端口
								</td>
								<td>
									<input type="text" name="proxyDevPort" id="proxyDevPort"
										value="${requestScope.result.data.proxyDevPort == -1 ? '' : requestScope.result.data.proxyDevPort }" />
								</td>
							</tr>
							<tr>
								<td>
									采集类型
								</td>
								<td>
									<input type="hidden" id="collectType"
										value="${requestScope.result.data.collectType.value }" />
									<select name="collectType">
										<option value="1">
											telnet
										</option>
										<option value="2">
											tcp
										</option>
										<option value="3">
											ftp
										</option>
										<option value="4">
											本地文件
										</option>
										<option value="5">
											数据库
										</option>
									</select>
								</td>
							</tr>
							<tr>
								<td>
									采集周期
								</td>
								<td>
									<input type="hidden" id="collectPeriod"
										value="${requestScope.result.data.collectPeriod.value }" />
									<select name="collectPeriod">
										<option value="1">
											一直
										</option>
										<option value="2">
											天
										</option>
										<option value="3">
											小时
										</option>
										<option value="4">
											半小时
										</option>
										<option value="5">
											15分钟
										</option>
										<option value="6">
											4小时
										</option>
										<option value="7">
											5分钟
										</option>
										<option value="8">
											12小时
										</option>
									</select>
								</td>
							</tr>
							<tr>
								<td>
									采集超时
								</td>
								<td>
									<input type="text" name="collectTimeout" id="collectTimeout"
										value="${requestScope.result.data.collectTimeout == -1 ? '' : requestScope.result.data.collectTimeout }" />
								</td>
							</tr>
							<tr>
								<td>
									组编号
								</td>
								<td>
									<input type="text" name="groupId" id="groupId"
										value="${requestScope.result.data.groupId == -1 ? '' : requestScope.result.data.groupId }" />
								</td>
							</tr>
							<tr>
								<td>
									命令超时时间
								</td>
								<td>
									<input type="text" name="shellTimeout" id="shellTimeout"
										value="${requestScope.result.data.shellTimeout == -1 ? '' : requestScope.result.data.shellTimeout }" />
								</td>
							</tr>
							<tr>
								<td>
									解析模板号
								</td>
								<td>
									<input type="text" name="parseTmpId" id="parseTmpId"
										value="${requestScope.result.data.parseTmpId }" />
								</td>
							</tr>
							<tr>
								<td>
									分发模板号
								</td>
								<td>
									<input type="text" name="distributeTmpId" id="distributeTmpId"
										value="${requestScope.result.data.distributeTmpId }" />
								</td>
							</tr>
							<tr>
								<td>
									采集时间
								</td>
								<td>
									<input type="text" name="collectTime" id="collectTime"
										value="${requestScope.result.data.collectTime == -1 ? '' : requestScope.result.data.collectTime }" />
								</td>
							</tr>

							<tr>
								<td>
									当前数据位置
								</td>
								<td>
									<input type="text" name="sucDataPos" id="sucDataPos"
										value="${requestScope.result.data.sucDataPos }" />
								</td>
							</tr>

							<tr>
								<td>
									是否更新
								</td>
								<td>
									<input type="text" name="isUpdate" id="isUpdate"
										value="${requestScope.result.data.isUpdate }" />
								</td>
							</tr>
						</table>
					</td>
					<td valign="top">
						<table>

							<tr>
								<td>
									重采次数
								</td>
								<td>
									<input type="text" name="maxCltTime" id="maxCltTime"
										value="${requestScope.result.data.maxCltTime }" />
								</td>
							</tr>
							<tr>
								<td>
									前置命令
								</td>
								<td>
									<input type="text" name="shellCmdPrepare" id="shellCmdPrepare"
										value="${requestScope.result.data.shellCmdPrepare }" />
								</td>
							</tr>
							<tr>
								<td>
									后置命令
								</td>
								<td>
									<input type="text" name="shellCmdFinish" id="shellCmdFinish"
										value="${requestScope.result.data.shellCmdFinish }" />
								</td>
							</tr>
							<tr>
								<td>
									时间偏移量
								</td>
								<td>
									<input type="text" name="collectTimepos" id="collectTimepos"
										value="${requestScope.result.data.collectTimepos }" />
								</td>
							</tr>
							<tr>
								<td>
									数据库驱动
								</td>
								<td>
									<input type="text" name="dbDriver" id="dbDriver"
										value="${requestScope.result.data.dbDriver }" />
								</td>
							</tr>
							<tr>
								<td>
									数据库URL
								</td>
								<td>
									<input type="text" name="dbUrl" id="dbUrl"
										value="${requestScope.result.data.dbUrl }" />
								</td>
							</tr>
							<tr>
								<td>
									线程休眠时间
								</td>
								<td>
									<input type="text" name="threadSleepTime" id="threadSleepTime"
										value="${requestScope.result.data.threadSleepTime }" />
								</td>
							</tr>
							<tr>
								<td>
									超时时间
								</td>
								<td>
									<input type="text" name="blockTime" id="blockTime"
										value="${requestScope.result.data.blockTime }" />
								</td>
							</tr>
							<tr>
								<td>
									采集机名称
								</td>
								<td>
									<input type="text" name="collectorName" id="collectorName"
										value="${requestScope.result.data.collectorName }" />
								</td>
							</tr>
							<tr>
								<td>
									记录数
								</td>
								<td>
									<input type="text" name="paramRecord" id="paramRecord"
										value="${requestScope.result.data.paramRecord }" />
								</td>
							</tr>
							<tr>
								<td>
									当前时间点
								</td>
								<td>
									<input type="text" name="sucDataTime" id="sucDataTime"
										value="<fmt:formatDate
										value="${requestScope.result.data.sucDataTime }"
										pattern="yyyy-MM-dd HH:mm:ss" />"
										onfocus="calendar();" />
								</td>
							</tr>
							<tr>
								<td>
									结束时间点
								</td>
								<td>
									<input type="text" name="endDataTime" id="endDataTime"
										value="<fmt:formatDate
										value="${requestScope.result.data.endDataTime }"
										pattern="yyyy-MM-dd HH:mm:ss" />"
										onfocus="calendar();" />

								</td>
							</tr>
							<tr>
								<td>
									解析器编号
								</td>
								<td>
									<input type="text" name="parserId" id="parserId"
										value="${requestScope.result.data.parserId }" />
								</td>
							</tr>
							<tr>
								<td>
									分发器编号
								</td>
								<td>
									<input type="text" name="distributorId" id="distributorId"
										value="${requestScope.result.data.distributorId }" />
								</td>
							</tr>
							<tr>
								<td>
									补采时间偏移量
								</td>
								<td>
									<input type="text" name="redoTimeOffset" id="redoTimeOffset"
										value="${requestScope.result.data.redoTimeOffset }" />
								</td>
							</tr>
							<tr>
								<td></td>
								<td>
									<input type="button" value="返回" id="back" />
									&nbsp;&nbsp;&nbsp;&nbsp;
									<input type="button" id="save" value="保存" />
								</td>
							</tr>
						</table>
					</td>
					<td valign="top">
						<table width="100%" border="0">
							<tr>
								<td scope="col">
									采集路径
								</td>
							</tr>
							<tr>
								<td>
									<textarea name="collectPath" id="collectPath" rows="20"
										cols="40">${requestScope.result.data.collectPath }</textarea>
								</td>
							</tr>
						</table>
					</td>
				</tr>
			</table>
		</form>
	</body>
</html>