<%@ page language="java" pageEncoding="utf-8"%>
<%@taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<html>
	<head>
		<title>添加补采任务</title>
		<meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
		<link href="/css/igp.css" rel="stylesheet" type="text/css" />
		<style type="text/css">
.currentTitle {
	font-size: 18px;
	font-weight: bold;
}

body {
	font: 12px/ 19px Arial, Helvetica, sans-serif;
	color: #666;
}

form div {
	margin: 10px 0;
}

.int label {
	float: left;
	width: 100px;
	text-align: right;
}

.int input {
	padding: 1px 1px;
	border: 1px solid #ccc;
	height: 25px;
	width: 250px;
}

.sub {
	padding-left: 100px;
}

.formtips {
	width: 350px;
	margin: 2px;
	padding: 2px;
}

.onError {
	background: #FFE0E9 url(../images/reg3.gif) no-repeat 0 center;
	padding-left: 25px;
}

.onSuccess {
	background: #E9FBEB url(../images/reg4.gif) no-repeat 0 center;
	padding-left: 25px;
}

.high {
	color: red;
}
</style>
		<script type="text/javascript" src="../js/jquery.js"></script>
		<script language="javascript" charset="gb2312" type="text/javascript" src="../js/DateTimeCalendar.js"></script>
		<script type="text/javascript" language="javascript">
		$(function(){
			//如果是必填的，则加红星标识.
			$("form :input.required").each(function(){
				var required = $("<strong class='high'> *</strong>"); //创建元素
				$(this).parent().append(required); //然后将它追加到文档中
			});
			    //文本框失去焦点后
		    $('form :input').blur(function(){
				 var $parent = $(this).parent();
				 $parent.find(".formtips").remove();
			
				//验证采集时间
				if($(this).is('#collectTime')){
					var val = this.value;
					if(val!=""&&/^\d{4}(-\d{2}){2}(\s\d{2}(:\d{2}){2})?$/.test(val)&&val.length>=10&&val.length<=19){
					    var okMsg = '输入正确.';
					    $parent.append('<span class="formtips onSuccess">'+okMsg+'</span>');
					}else{
					    var errorMsg = '请输入正确日期：如：1970-01-01 00:00:00或1970-01-01';
	                    $parent.append('<span class="formtips onError">'+errorMsg+'</span>');
					}
				}
			
				//验证机器名
				if($(this).is('#collectorName')){
					if( this.value==""){
					    var errorMsg = '机器名不能为空！';
	                       $parent.append('<span class="formtips onError">'+errorMsg+'</span>');
					}else{
					    var okMsg = '输入正确.';
					    $parent.append('<span class="formtips onSuccess">'+okMsg+'</span>');
					}
				}
				//验证补采优先级
				if($(this).is('#readoptType')){
					var val = this.value;
					if(val==""||(val!=""&&/^(0|[1-9]\d*)$/.test(val)&&val>=0&&val<=2147483647)){
					    var okMsg = '输入正确,默认为0！';
					    $parent.append('<span class="formtips onSuccess">'+okMsg+'</span>');
					}else{
					    var errorMsg = '只能为空或数字.';
	                    $parent.append('<span class="formtips onError">'+errorMsg+'</span>');
					}
				}
			}).keyup(function(){
			   $(this).triggerHandler("blur");
			}).focus(function(){
		  	   $(this).triggerHandler("blur");
			});//end blur
	
			//提交，最终验证。
			 $('#add').click(function(){
			 	var taskId = $("form select").val();
			 	if(taskId=="请选择任务"){
			 		alert("请选择任务！");
			 		return;
			 	}
				$("form :input.required").trigger('blur');
				var numError = $('form .onError').length;
				if(numError==0){
					$('#action').val('add');
					$('#forwardURL').val('/page/result.jsp');
					$('#returnURL').val('/page/rTask.jsp');
					document.formAdd.submit();
				} 
			 });
			 //查询任务的路径
			 $("#getTaskFilepath").click(function(){
			 	var taskId = $("form select").val();
			 	if(taskId=="请选择任务"){
			 		alert("请选择任务！");
			 		return;
			 	}
			 	$.post("/ToysServlet",{action:"getTaskFilePath",taskID:taskId},function(data){
			 		$("#filePath").val(data);
			 	});
			 });
			//重置
			 $('#res').click(function(){
					$(".formtips").remove(); 
			 });
		});
	</script>
	</head>
	<body>
		<form name="formAdd" action="/RTaskServlet" method="post">
			<input type="hidden" name="action" id="action">
			<input type="hidden" name="forwardURL" id="forwardURL">
			<input type="hidden" name="returnURL" id="returnURL">
			<table width="98%" border="0" style="margin-left:20px; margin-top:10px;">
				<tr>
					<th width="99%" scope="col"></th>
					<th width="1%" rowspan="2" scope="col"></th>
				</tr>
				<tr>
					<td valign="top">
						<span class="currentTitle">添加补采任务</span>
						<div class="int">
							<label>
								任务编号: 
					    </label>
							<select name="taskID">
								<option>
									请选择任务
								</option>
								<c:forEach items="${requestScope.result.data}" var="task">
									<option value="${task.taskId}">
										${task.taskDescribe} - ${task.taskId} - ${task.collectorName}
								  </option>
								</c:forEach>
							</select>
						</div>
						<div class="int">
							<label>
								数据时间: 
					    </label>
							<input type="text" id="collectTime" name="collectTime"
								class="required">
						</div>
						<div class="int">
							<label>
								机器名: 
					    </label>
							<input type="text" id="collectorName" name="collectorName"
								class="required" value="${requestScope.hostName}">
						</div>
						<div class="int">
							<label>
								优先级: 
					    </label>
							<input type="text" id="readoptType" name="readoptType"
								class="required">
						</div>
						<div class="int">
							<label>
								采集路径: 
						  </label>&nbsp; &nbsp;
								&nbsp;
						  <a href="#" id="getTaskFilepath">查看正常任务的采集路径</a> &nbsp;&nbsp;<a href="/page/dsExprTest.jsp">[数据源表达式匹配测试]</a>
						</div>
						<div class="int">
							<textarea rows="13" cols="115" id="filePath" name="filePath">${requestScope.filePath }</textarea>
						</div>
						<div class="sub" align="left">
							<button type="button" id="add">
								提交
							</button>
							&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;
							<button type="reset" id="res">
								重置
							</button>
							&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;
				    <button type="button" id="return" onClick="window.location.href='${requestScope.result.returnURL}';">
								返回
							</button>
							&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;
					  </div>
					</td>
				</tr>
			</table>
		</form>
	</body>
</html>
