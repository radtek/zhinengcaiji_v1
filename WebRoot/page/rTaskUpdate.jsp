<%@ page language="java" pageEncoding="utf-8"%>
<%@taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<html>
	<head>
		<title>更新补采任务</title>
		<meta http-equiv="pragma" content="no-cache">
		<meta http-equiv="cache-control" content="no-cache">
		<meta http-equiv="expires" content="0">
		<meta http-equiv="keywords" content="keyword1,keyword2,keyword3">
		<meta http-equiv="description" content="This is my page">
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
	margin: 5px 0;
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

				 //验证任务号
				if($(this).is('#taskID')){
					var val = this.value;
					if(val!=""&&/^(0|[1-9]\d*)$/.test(val)&&val>=0&&val<=2147483647){
					    var okMsg = '输入正确.';
					    $parent.append('<span class="formtips onSuccess">'+okMsg+'</span>');
					}else{
					    var errorMsg = '请输入0-2147483647之间的数字.';
	                    $parent.append('<span class="formtips onError">'+errorMsg+'</span>');
					}
				}
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
				//验证补采类型
				if($(this).is('#readoptType')){
					var val = this.value;
					if(val==""||(val!=""&&/^(0|[1-9]\d*)$/.test(val)&&val>=0&&val<=2147483647)){
					    var okMsg = '输入正确.';
					    $parent.append('<span class="formtips onSuccess">'+okMsg+'</span>');
					}else{
					    var errorMsg = '只能为空或数字.';
	                    $parent.append('<span class="formtips onError">'+errorMsg+'</span>');
					}
				}
				//验证补采状态
				if($(this).is('#collectStatus')){
					var val = this.value;
					if(val==""||(val!=""&&/^(0|[1-9]\d*)$/.test(val)&&val>=0&&val<=2147483647)){
					    var okMsg = '输入正确.';
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
			
			$('#update').click(function(){
				$("form :input.required").trigger('blur');
				var numError = $('form .onError').length;
				if(numError!=0){
					alert("请正确输入");
					return;
				} 
				$('#action').val('update');
				$('#forwardURL').val('/page/result.jsp');
				$('#returnURL').val('/page/rTask.jsp');
				document.formUp.submit();
			});
		});
	</script>
	</head>
	<body>
    <c:set var="rTask" value="${requestScope.result.data}"></c:set>
		<form name="formUp" action="/RTaskServlet" method="post">
			<input type="hidden" name="action" id="action">
			<input type="hidden" name="forwardURL" id="forwardURL">
			<input type="hidden" name="returnURL" id="returnURL">
            <input type="hidden" name="id" id="id" value="${rTask.id}">
            <input type="hidden" name="stampTime" id="stampTime" value="${rTask.stampTime}">            
			<table width="100%" border="0">
				<tr>
					<td valign="top">
						<span class="currentTitle">编辑补采任务 <font color="#FF0000">${rTask.id}</font></span> &nbsp;&nbsp;<font color="#999999" size="-1">${rTask.stampTime}</font>
						<div class="int">
							<label>
								任务编号
							</label>
							<input type="text" id="taskID" name="taskID"
								value="${rTask.taskID }" class="required">
						</div>
						<div class="int">
							<label>
								数据时间
							</label>
							<input type="text" id="collectTime" name="collectTime"
								value="${rTask.collectTime }" class="required">
						</div>
						<div class="int">
							<label>
								机器名
							</label>
							<input type="text" id="collectorName" name="collectorName"
								value="${rTask.collectorName }" class="required">
						</div>
						<div class="int">
							<label>
								优先级
							</label>
							<input type="text" id="readoptType" name="readoptType"
								value="${rTask.readoptType }" class="required">
						</div>
						<div class="int">
							<label>
								状态
							</label>
							<select name="collectStatus">
								<option value="0" ${rTask.collectStatus
									eq 0 ? "selected='selected'" : "" }>
									新增任务
								</option>
								<option value="3" ${rTask.collectStatus
									eq 3 ? "selected='selected'" : "" }>
									历吏任务
								</option>
								<option value="-1" ${rTask.collectStatus
									eq -1 ? "selected='selected'" : "" }>
									失效任务
								</option>
							</select>
						</div>
						<div class="int">
							<label>
								补采原因
							</label>
							<textarea rows="7" cols="110" readonly="readonly" id="cause" name="cause">${rTask.cause }</textarea>
						</div>
						<div class="int">
							<label>
								采集路径
							</label>
							<textarea rows="7" cols="110" id="filePath" name="filePath">${rTask.filePath }</textarea>
						</div>
						<div class="sub" align="left">
							<button type="button" id="update">
								更新
							</button>
							&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;
							<button type="reset" id="cancel">
								重置
							</button>
                            &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;
							<button type="button" id="return" onClick="window.location.href='${requestScope.result.returnURL}';">
								返回
							</button>
						</div>
					</td>
				</tr>
			</table>
		</form>
	</body>
</html>