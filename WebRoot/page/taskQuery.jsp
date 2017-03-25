<%@ page language="java" pageEncoding="utf-8"%>
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">
<html>
	<head>
		<title>任务高级查询</title>
		<link href="/css/igp.css" rel="stylesheet" type="text/css" />
		<script type="text/javascript" src="/js/jquery.js"></script>
		<script type="text/javascript" src="/js/taskQuery.js"></script>
	</head>
	<body>
		<form action="/page/task" method="post">
			<input type="hidden" name="action" value="query" />
			<input type="hidden" name="currentPage" value="1" />

			<table width="95%" style="margin-left:10px; margin-top:10px;">
            <tr>
        <td height="30" colspan="3"><span class="currentTitle">任务高级查询</span></td>
        </tr>
        <tr>
        <td height="30" colspan="3" style="BORDER-BOTTOM: #d4d4d4 1px dashed; height: 2px;"></td>
        </tr>
				<tr>
					<td valign="top" width="300">
						<table id="detailTable">
							<tr>
								<td>
									任务号
								</td>
								<td>
									<input type="text" name="taskId" id="taskId" value="" />
								</td>
							</tr>
							<tr>
								<td>
									任务描述
								</td>
								<td>
									<input type="text" name="taskDescribe" id="taskDescribe"
										value="" />
								</td>
							</tr>
							<tr>
								<td>
									设备号
								</td>
								<td>
									<input type="text" name="devId" id="devId" value="" />
								</td>
							</tr>
							<tr>
								<td>
									设备端口
								</td>
								<td>
									<input type="text" name="devPort" id="devPort" value="" />
								</td>
							</tr>
							<tr>
								<td>
									代理设备号
								</td>
								<td>
									<input type="text" name="proxyDevId" id="proxyDevId" value="" />
								</td>
							</tr>
							<tr>
								<td>
									代理设备端口
								</td>
								<td>
									<input type="text" name="proxyDevPort" id="proxyDevPort"
										value="" />
								</td>
							</tr>
							<tr>
								<td>
									采集类型
								</td>
								<td>
									<select name="collectType">
										<option value="-1">
											请选择
										</option>
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
									<select name="collectPeriod">
										<option value="-1">
											请选择
										</option>
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
										value="" />
								</td>
							</tr>
							<tr>
								<td>
									采集时间
								</td>
								<td>
									<input type="text" name="collectTime" id="collectTime" value="" />
								</td>
							</tr>
							<tr>
								<td>
									命令超时时间
								</td>
								<td>
									<input type="text" name="shellTimeout" id="shellTimeout"
										value="" />
								</td>
							</tr>
							<tr>
								<td>
									解析模板号
								</td>
								<td>
									<input type="text" name="parseTmpId" id="parseTmpId" value="" />
								</td>
							</tr>
							<tr>
								<td>
									分发模板号
								</td>
								<td>
									<input type="text" name="distributeTmpId" id="distributeTmpId"
										value="" />
								</td>
							</tr>
							<tr>
								<td>
									当前时间点
								</td>
								<td>
									<input type="text" name="sucDataTime" id="sucDataTime" value="" />
								</td>
							</tr>
							<tr>
								<td>
									当前数据位置
								</td>
								<td>
									<input type="text" name="sucDataPos" id="sucDataPos" value="" />
								</td>
							</tr>
                            <tr>
								<td>
									分发器编号
								</td>
								<td>
									<input type="text" name="distributorId" id="distributorId"
										value="" />
								</td>
							</tr>
						</table>
					</td>
					<td valign="top" width="300">
						<table>
							<tr>
								<td>
									是否启用
								</td>
								<td>
									<input type="text" name="isUsed" id="isUsed" value="" />
								</td>
							</tr>
							<tr>
								<td>
									是否更新
								</td>
								<td>
									<input type="text" name="isUpdate" id="isUpdate" value="" />
								</td>
							</tr>
							<tr>
								<td>
									重采次数
								</td>
								<td>
									<input type="text" name="maxCltTime" id="maxCltTime" value="" />
								</td>
							</tr>
							<tr>
								<td>
									前置命令
								</td>
								<td>
									<input type="text" name="shellCmdPrepare" id="shellCmdPrepare"
										value="" />
								</td>
							</tr>
							<tr>
								<td>
									后置命令
								</td>
								<td>
									<input type="text" name="shellCmdFinish" id="shellCmdFinish"
										value="" />
								</td>
							</tr>
							<tr>
								<td>
									时间偏移量
								</td>
								<td>
									<input type="text" name="collectTimepos" id="collectTimepos"
										value="" />
								</td>
							</tr>
							<tr>
								<td>
									数据库驱动
								</td>
								<td>
									<input type="text" name="dbDriver" id="dbDriver" value="" />
								</td>
							</tr>
							<tr>
								<td>
									数据库URL
								</td>
								<td>
									<input type="text" name="dbUrl" id="dbUrl" value="" />
								</td>
							</tr>
							<tr>
								<td>
									线程休眠时间
								</td>
								<td>
									<input type="text" name="threadSleepTime" id="threadSleepTime"
										value="" />
								</td>
							</tr>
							<tr>
								<td>
									超时时间
								</td>
								<td>
									<input type="text" name="blockTime" id="blockTime" value="" />
								</td>
							</tr>
							<tr>
								<td>
									采集机名称
								</td>
								<td>
									<input type="text" name="collectorName" id="collectorName"
										value="" />
								</td>
							</tr>
							<tr>
								<td>
									记录数
								</td>
								<td>
									<input type="text" name="paramRecord" id="paramRecord" value="" />
								</td>
							</tr>
							<tr>
								<td>
									组编号
								</td>
								<td>
									<input type="text" name="groupId" id="groupId" value="" />
								</td>
							</tr>
							<tr>
								<td>
									结束时间点
								</td>
								<td>
									<input type="text" name="endDataTime" id="endDataTime" value="" />
								</td>
							</tr>
							<tr>
								<td>
									解析器编号
								</td>
								<td>
									<input type="text" name="parserId" id="parserId" value="" />
								</td>
							</tr>
							
							<tr>
								<td>
									补采时间偏移量
								</td>
								<td>
									<input type="text" name="redoTimeOffset" id="redoTimeOffset"
										value="" />
								</td>
							</tr>
							<tr>
								<td></td>
								<td>
									<input type="button" value="返回" id="back" />
									&nbsp;&nbsp;&nbsp;&nbsp;
									<input type="button" value="查询" id="query" />
								</td>
							</tr>
						</table>
					</td>
					<td valign="top"><table width="100%" border="0">
				  <tr>
				    <td scope="col">采集路径</td>
			      </tr>
				  <tr>
				    <td><textarea name="collectPath" id="collectPath" rows="20" cols="40"></textarea></td>
			      </tr>
				  <tr>
				    <td><a href="/page/dsExprTest.jsp">[数据源表达式匹配测试]</a></td>
				    </tr>
		          </table></td>
				</tr>

			</table>

		</form>
	</body>
</html>
