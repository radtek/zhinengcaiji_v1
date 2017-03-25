<%@ page language="java" contentType="text/html; charset=utf-8"
	pageEncoding="utf-8"%>
<%@taglib prefix="fmt" uri="http://java.sun.com/jsp/jstl/fmt"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
	<head>
		<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
		<title>任务详情</title>
		<link href="/css/igp.css" rel="stylesheet" type="text/css" />
		<script type="text/javascript" src="/js/jquery.js"></script>
		<script type="text/javascript" src="/js/taskDetail.js"></script>
	</head>
	<body>
		<input type="hidden" value="${requestScope.result.returnURL }"
			id="returnUrl" />
		<table width="95%" style="margin-left:10px; margin-top:10px;">
        <tr>
        <td height="30" colspan="3"><span class="currentTitle">${requestScope.result.data.taskDescribe} <font color="#FF0000">${requestScope.result.data.taskId}</font></span></td>
        </tr>
        <tr>
        <td height="30" colspan="3" style="BORDER-BOTTOM: #d4d4d4 1px dashed; height: 2px;"></td>
        </tr>
			<tr>
				<td valign="top" width="300">
					<table id="detailTable">
						<tr>
							<td width="96">
								是否启用
							</td>
							<td width="287">
								<font color="#FF0000">${requestScope.result.data.isUsed }</font>
							</td>
						</tr>
						<tr>
							<td>
								是否更新
							</td>
							<td>
								${requestScope.result.data.isUpdate }
							</td>
						</tr>
						<tr>
							<td>
								设备号
							</td>
							<td>
								${requestScope.result.data.devId }
							</td>
						</tr>
						<tr>
							<td>
								设备端口
							</td>
							<td>
								${requestScope.result.data.devPort }
							</td>
						</tr>
						<tr>
							<td>
								代理设备号
							</td>
							<td>
								${requestScope.result.data.proxyDevId }
							</td>
						</tr>
						<tr>
							<td>
								代理设备端口
							</td>
							<td>
								${requestScope.result.data.proxyDevPort }
							</td>
						</tr>
						<tr>
							<td>
								采集类型
							</td>
							<td>
								${requestScope.result.data.collectType.name }
							</td>
						</tr>
						<tr>
							<td>
								采集周期
							</td>
							<td>
								${requestScope.result.data.collectPeriod.name }
							</td>
						</tr>
						<tr>
							<td>
								采集超时
							</td>
							<td>
								${requestScope.result.data.collectTimeout }
							</td>
						</tr>
						<tr>
							<td>
								采集时间
							</td>
							<td>
								${requestScope.result.data.collectTime }
							</td>
						</tr>
						<tr>
							<td>
								命令超时时间
							</td>
							<td>
								${requestScope.result.data.shellTimeout }
							</td>
						</tr>
						<tr>
							<td>
								解析模板号
							</td>
							<td>
								${requestScope.result.data.parseTmpId }
							</td>
						</tr>
						<tr>
							<td>
								分发模板号
							</td>
							<td>
								${requestScope.result.data.distributeTmpId }
							</td>
						</tr>
						<tr>
							<td>
								分组编号
							</td>
							<td>
								${requestScope.result.data.groupId }
							</td>
						</tr>
						<tr>
							<td>
								当前数据位置
							</td>
							<td>
								${requestScope.result.data.sucDataPos }
							</td>
						</tr>
                        <tr>
							<td>&nbsp;</td>
							<td>&nbsp;</td>
						</tr>
					</table>
				</td>
				<td valign="top" width="370">
					<table>
						<tr>
							<td width="113">
								重采次数
							</td>
							<td width="276">
								${requestScope.result.data.maxCltTime }
							</td>
						</tr>
						<tr>
							<td>
								前置命令
							</td>
							<td>
								${requestScope.result.data.shellCmdPrepare }
							</td>
						</tr>
						<tr>
							<td>
								后置命令
							</td>
							<td>
								${requestScope.result.data.shellCmdFinish }
							</td>
						</tr>
						<tr>
							<td>
								时间偏移量
							</td>
							<td>
								${requestScope.result.data.collectTimepos }
							</td>
						</tr>
						<tr>
							<td>
								数据库驱动
							</td>
							<td>
								${requestScope.result.data.dbDriver }
							</td>
						</tr>
						<tr>
							<td>
								数据库URL
							</td>
							<td>
								${requestScope.result.data.dbUrl }
							</td>
						</tr>
						<tr>
							<td>
								线程休眠时间
							</td>
							<td>
								${requestScope.result.data.threadSleepTime }
							</td>
						</tr>
						<tr>
							<td>
								超时时间
							</td>
							<td>
								${requestScope.result.data.blockTime }
							</td>
						</tr>
						<tr>
							<td>
								采集机名称
							</td>
							<td>
								<font color="#FF0000">${requestScope.result.data.collectorName }</font>
							</td>
						</tr>
						<tr>
							<td>
								记录数
							</td>
							<td>
								${requestScope.result.data.paramRecord }
							</td>
						</tr>
						<tr>
							<td>
								当前时间点
							</td>
							<td>
								<font color="#FF0000"><fmt:formatDate value="${requestScope.result.data.sucDataTime }"
									pattern="yyyy-MM-dd HH:mm:ss" /></font>
							</td>
						</tr>
						<tr>
							<td>
								结束时间点
							</td>
							<td>
								<fmt:formatDate value="${requestScope.result.data.endDataTime }"
									pattern="yyyy-MM-dd HH:mm:ss" />
							</td>
						</tr>
						<tr>
							<td>
								解析器编号
							</td>
							<td>
								${requestScope.result.data.parserId }
							</td>
						</tr>
						<tr>
							<td>
								分发器编号
							</td>
							<td>
								${requestScope.result.data.distributorId }
							</td>
						</tr>
						<tr>
							<td>
								补采时间偏移量
							</td>
							<td>
								${requestScope.result.data.redoTimeOffset }
							</td>
						</tr>
						<tr>
							<td height="30"></td>
							<td>
								<input type="button" value="返回" id="back" />
							</td>
						</tr>
					</table>
				</td>
				<td valign="top"><table width="100%" border="0">
				  <tr>
				    <td scope="col">采集路径</td>
			      </tr>
				  <tr>
				    <td><textarea rows="20" cols="40" readonly="readonly">${requestScope.result.data.collectPath }</textarea></td>
			      </tr>
			    </table></td>
			</tr>
		</table>
	</body>
</html>