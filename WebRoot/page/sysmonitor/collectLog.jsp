<%@ page language="java" pageEncoding="utf-8"%>
<%@taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<script type="text/javascript">
  function getValidate(){
     var goValue = document.getElementById("inputPage").value;
     if(goValue ==""){
        alert("页数不能为空.");
        return false;
     }
     if (isNaN(goValue)){
		alert("页数必须是数字.");
		return false;
	}
	if(goValue <= 0){
	  alert("页数必须大于0.");
		return false;
	}
     window.location.href='collectLog.do?action=queryPage&id=inputPage&go='+goValue+'';
  }
  
  function getValidateId(){
     var idValue = document.getElementById("taskId").value;
    
     if (isNaN(idValue)){
		alert("任务ID必须是数字.");
		return false;
	}
  }
</script>
<html>
	<head>
		<META content=no-cache http-equiv=Pragma>
		<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
		<title>采集日志</title>
		<link href="/css/igp.css" rel="stylesheet" type="text/css" />
		<script language="javascript" src="/js/jquery.js"></script>
		<script language="javascript" src="/js/sysmonitor/summaryInterface.js"></script>
		<style type="text/css">
.searchBox {
	float: left;
	margin-bottom: 5px;
	color: #090;
	font-weight: bold;
}

.thead-left {
	
}

.thead-left TD {
	text-align: left;
}

.currentTitle {
	font-size: 18px;
	font-weight: bold;
}
</style>
		<script type="text/javascript" src="/js/jquery.js"></script>
		<script language="javascript" charset="gb2312" type="text/javascript"
			src="/js/DateTimeCalendar.js"></script>
		<script type="text/javascript" charset="gb2312" src="/js/rtask.js"></script>
	</head>

	<body>
		<form action="collectLog.do?action=selectLog" method="post" id="sumForm">
			<input name="action" id="action" value="query" type="hidden" />

			<table width="100%" border="0">
				<tr>
					<th width="78%" scope="col">
						&nbsp;
					</th>
					<th width="2%" rowspan="2" scope="col"
						style="BORDER-RIGHT: #d4d4d4 1px dashed;">
						&nbsp;
					</th>
					<th width="20%" scope="col">
						&nbsp;
					</th>
				</tr>
				<tr>
					<td valign="top">
						<!--list start-->
						<table width="1000" style="margin-left: 20px;">
							<tr class="thead-left">
								<td colspan="7" height="60">
									<span class="currentTitle">采集日志记录表"IGP_DATA_LOG" <font
										color="#999999">(只读)</font> </span>
								</td>
							</tr>
							<tr class="thead-left">
								<td width="15%">
									当前日志的时间
								</td>
								<td width="10%">
									任务号
								</td>
								<td width="12%">
									任务描述
								</td>
								<td width="8%">
									任务类型
								</td>
								<td width="8%">
									任务状态
								</td>
								<td width="15%">
									采集的时间点
								</td>
								<td width="8%">
									消耗时间
								</td>
								<td width="8%">
									采集结果
								</td>	
								<td width="10%">
				                      详情      
								</td>
								<td width="12%">
									异常信息
								</td>
							</tr>
							<c:forEach items="${requestScope.result.data.datas }" var="pojo">
								<tr onMouseOver="JavaScript:this.style.background='#ffdfc1'" onMouseOut="JavaScript:this.style.background='#ffffff'">
									<td>
										${pojo.logTime }
									</td>
									<td>
										${pojo.taskId }
									</td>
									<td>
										${pojo.taskDescription }
									</td>
									<td>
										${pojo.taskType }
									</td>
									<td>
										${pojo.taskStatus }
									</td>
									<td>
										${pojo.dataTime }
									</td>
									<td>
										${pojo.costTime }
									</td>
									<td>
										${pojo.taskResult }
									</td>
									<td>
										${pojo.taskDetail }
									</td>
									<td>
										${pojo.taskException }
									</td>
								</tr>
							</c:forEach>
							<tr>
								<td colspan="7"
									style="BORDER-BOTTOM: #d4d4d4 1px dashed; height: 2px;">
									<input type="hidden" name="page" id="page" value="${requestScope.result.data.currentPage}">
									<a href="collectLog.do?action=queryPage&id=firstPage">&lt;&lt;首页</a>&nbsp;&nbsp;&nbsp;
									<a href="collectLog.do?action=queryPage&id=prePage&pageNum=${requestScope.result.data.currentPage}" >&lt;&lt;上一页</a>&nbsp;&nbsp;&nbsp;
									<a href="collectLog.do?action=queryPage&id=nextPage&pageNum=${requestScope.result.data.currentPage}" >下一页&gt;&gt;</a>&nbsp;&nbsp;&nbsp;
									<a href="collectLog.do?action=queryPage&id=lastPage">末页&gt;&gt;</a>&nbsp;&nbsp;&nbsp;
									当前第${requestScope.result.data.currentPage
									} 页/共${requestScope.result.data.pageCount
									} 页&nbsp;&nbsp;&nbsp;&nbsp;到第
									<input type="text" size="2" maxlength="5" id="inputPage" />
									页
									<a id="go" onclick="return getValidate()">go</a>&nbsp;&nbsp;
								</td>
							</tr>

						</table>
						<!--list end-->
					</td>
					<td style="padding-left: 10px;" valign="top">
						<table width="100%" border="0" style="margin-top: 10px;">
							<tr>
								<td>
									<span class="navLink"><a href="index.jsp"
										title="点击返系统监视主界面"> &gt;&gt;返回 </a> </span>
								</td>
							</tr>
						</table>
						<table width="100%" border="0">
						   <!--	
							<tr>
								<th scope="col">
									<input type="text" id="selectKeyWord" name="selectKeyWord" class="searchBox" />
								</th>
							</tr>
							-->
							<tr>
							   <td align="left">
									<label>
										任务号
									</label>
									<input type="text" id="taskId" name="taskId"/>
								</td>
							</tr>
							<tr>
							   <td align="left">
									<label>
										采集数据开始时间
									</label>
									<input type="text" id="stampStartTime" name="stampStartTime" onfocus="calendar();" value="${requestScope.stampStartTime }"/>
								</td>
							</tr>
							<tr>
							   <td align="left">
									<label>
										采集数据结束时间
									</label>
									<input type="text" id="stampEndTime" name="stampEndTime" onfocus="calendar();" value="${requestScope.stampEndTime }"/>
								</td>
							</tr>							
							<tr>
								<td>
									<label>
											任务类型
									</label>
									<select name="taskType" id="taskType">
										<option value="">
											
										</option>
										<option value="task">
											正常任务
										</option>
										<option value="rTask">
											补采任务
										</option>
									
								</td>
							</tr>	
							<tr>
								<td>
									<label>
											采集结果
									</label>
									<select name="taskResult" id="taskResult">
										<option value="">
											
										</option>
										<option value="success">
											成功
										</option>
										<option value="partSuccess">
											部分成功
										</option>
										<option value="fail">
											失败
										</option>
								</td>
							</tr>
							<tr>
								<td>
									<label>
											异常信息
									</label>
									<select name="taskException" id="taskException">
										<option value="">
											
										</option>
										<option value="yes">
											有异常信息
										</option>
										<option value="no">
											无异常信息
										</option>
									
								</td>
							</tr>				
						<!--	
							<tr>
								<td>
									<select name="selectType" id="selectType">
										<option value="">
											请选择
										</option>
										<option value="taskid">
											任务ID
										</option>
										<option value="stamptime">
											采集数据时间
										</option>
										<option value="taskResult">
											采集结果
										</option>
										<option value="taskException">
											异常信息
										</option>
									</select>
									&nbsp;
									<input type="submit" id="query" value="查询" />
								</td>
							</tr>
							-->
						 <tr>
								<td>
									&nbsp;
									<input type="submit" id="query" value="查询" onclick="return getValidateId()"/>
								</td>
							</tr>
							<tr>
								<td></td>
							</tr>
						</table>
					</td>
				</tr>
			</table>
		</form>
	</body>
</html>