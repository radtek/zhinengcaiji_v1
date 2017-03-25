<%@ page contentType="text/html; charset=utf-8" language="java" import="java.util.*" errorPage="" %>
<%@taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<%
	/**
	  *参数设置
	  *Author:YangJian
	  *Since: 1.0
	  *2010-6-2
	**/
%>
<html>
<head>
<title>参数设置</title>
<meta http-equiv="pragma" content="no-cache">
<meta http-equiv="cache-control" content="no-cache">
<meta http-equiv="expires" content="0">
<meta http-equiv="keywords" content="keyword1,keyword2,keyword3">
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
<link href="/css/igp.css" rel="stylesheet" type="text/css" />
	<script type="text/javascript" src="../js/jquery.js"></script>
	<style type="text/css">
<!--
.thead-left{
	
}

.thead-left TD{
	text-align:left;
}
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

.currentTitle{
	font-size:18px;
	font-weight:bold;
}
.high {
	color: red;
}

.int label {
	float: left;
	width: 130px;
	text-align: right;
}

.int input {
	padding: 1px 1px;
	border: 1px solid #ccc;
	height: 25px;
	width: 200px;
}

.sub {
	padding-left: 100px;
}

.formtips {
	width: 250px;
	margin: 1px;
	padding: 1px;
}

.onError {
	background: #FFE0E9 url(../images/reg3.gif) no-repeat 0 center;
	padding-left: 18px;
}

.onSuccess {
	background: #E9FBEB url(../images/reg4.gif) no-repeat 0 center;
	padding-left: 18px;
}
.h4{
  padding-left: 18px;
  float:left;
}

-->
</style>
<script language="javascript" >

$(function(){
			//如果是必填的，则加红星标识.
			$("#currentPath ,#templetFilePath,#maxCltCount,#maxRecltCount,#maxCountPerRegather,#maxActive").each(function(){
				var required = $("<strong class='high'> *</strong>"); //创建元素
				$(this).parent().append(required); //然后将它追加到文档中
				
			});
			    //文本框失去焦点后
		    $('form :input').blur(function(){
		    
				 var $parent = $(this).parent();
				 $parent.find(".formtips").remove();
				 //
				if($(this).is('#fieldMatch')){
				   
					var val = this.value;
					
					if(val!=""&&/^[1-9][0-9]*$|^(?:[1-9][0-9]*\.[0-9]+|0\.(?!0+$)[0-9]+)$/.test(val)&&val>=0&&val<=1){
					    var okMsg = '输入正确.';
					    $parent.append('<span class="formtips onSuccess">'+okMsg+'</span>');
					   
					}else{
					    var errorMsg = '请输入0-1之间的数字.';
	                    $parent.append('<span class="formtips onError">'+errorMsg+'</span>');
					}
				}
				if($(this).is('#port')){
				   
					var val = this.value;
					
					if(val!=""&&/^(0|[1-9]\d*)$/.test(val)&&val>=0&&val<=65535){
					    var okMsg = '输入正确.';
					    $parent.append('<span class="formtips onSuccess">'+okMsg+'</span>');
					  
					}else{
					    var errorMsg = '请输入0-65535之间的数字.';
	                    $parent.append('<span class="formtips onError">'+errorMsg+'</span>');
					}
				}
				
				if($(this).is('#maxThreadCount')){
				   
					var val = this.value;
					
					if(val!=""&&/^(0|[1-9]\d*)$/.test(val)&&val>=0&&val<=200){
					    var okMsg = '输入正确.';
					    $parent.append('<span class="formtips onSuccess">'+okMsg+'</span>');
					  
					}else{
					    var errorMsg = '请输入0-200之间的数字.';
	                    $parent.append('<span class="formtips onError">'+errorMsg+'</span>');
					}
				}
				
		    	if($(this).is('#maxCountPerRegather')){
				   
					var val = this.value;
					 
					if(val!=""&&/^(0|[1-9]\d*)$/.test(val)&&val>=0&&val<=500){
					    var okMsg = '输入正确.';
					    $parent.append('<span class="formtips onSuccess">'+okMsg+'</span>');
					   
					}else{
					    var errorMsg = '请输入0-500之间的数字.';
	                    $parent.append('<span class="formtips onError">'+errorMsg+'</span>');
					}
				}
				if($(this).is('#maxActive')){
				   
					var val = this.value;
					
					if(val!=""&&/^(0|[1-9]\d*)$/.test(val)&&val>=0&&val<=200){
					    var okMsg = '输入正确.';
					    $parent.append('<span class="formtips onSuccess">'+okMsg+'</span>');
					  
					}else{
					    var errorMsg = '请输入0-200之间的数字.';
	                    $parent.append('<span class="formtips onError">'+errorMsg+'</span>');
					}
				}
				
				if($(this).is('#maxCltCount')){
				   
					var val = this.value;
					
					if(val!=""&&/^(0|[1-9]\d*)$/.test(val)&&val>=0&&val<=200){
					    var okMsg = '输入正确.';
					    $parent.append('<span class="formtips onSuccess">'+okMsg+'</span>');
					  
					}else{
					    var errorMsg = '请输入0-200之间的数字.';
	                    $parent.append('<span class="formtips onError">'+errorMsg+'</span>');
					}
				}
				
				if($(this).is('#maxRecltCount')){
				   
					var val = this.value;
					
					if(val!=""&&/^(0|[1-9]\d*)$/.test(val)&&val>=0&&val<=200){
					    var okMsg = '输入正确.';
					    $parent.append('<span class="formtips onSuccess">'+okMsg+'</span>');
					  
					}else{
					    var errorMsg = '请输入0-200之间的数字.';
	                    $parent.append('<span class="formtips onError">'+errorMsg+'</span>');
					}
				}
				
	          if($(this).is('#lifecycle')){
				   
					var val = this.value;
					
					if(val!=""&&/^(0|[1-9]\d*)$/.test(val)&&val>=0&&val<=8640){
					    var okMsg = '输入正确.';
					    $parent.append('<span class="formtips onSuccess">'+okMsg+'</span>');
					  
					}else{
					    var errorMsg = '请输入0-8640之间的数字.';
	                    $parent.append('<span class="formtips onError">'+errorMsg+'</span>');
					}
				}
					 
				
				if($(this).is('#maxIdle')){
				   
					var val = this.value;
					
					if(val!=""&&/^(0|[1-9]\d*)$/.test(val)&&val>=0&&val<=200){
					    var okMsg = '输入正确.';
					    $parent.append('<span class="formtips onSuccess">'+okMsg+'</span>');
					  
					}else{
					    var errorMsg = '请输入0-200之间的数字.';
	                    $parent.append('<span class="formtips onError">'+errorMsg+'</span>');
					}
				}
			
				if($(this).is('#maxWait')){
				   
					var val = this.value;
					
					if(val!=""&&/^(0|[1-9]\d*)$/.test(val)&&val>=0&&val<=50000){
					    var okMsg = '输入正确.';
					    $parent.append('<span class="formtips onSuccess">'+okMsg+'</span>');
					  
					}else{
					    var errorMsg = '请输入0-50000之间的数字.';
	                    $parent.append('<span class="formtips onError">'+errorMsg+'</span>');
					}
				}
				

			}).keyup(function(){
			   $(this).triggerHandler("blur");
			}).focus(function(){
		  	   $(this).triggerHandler("blur");
			});//end blur
	
			//提交，最终验证。
			 $('#systemButton').click(function(){
				var numError = $('form .onError').length;
				alert("a"+numError);
				if(numError==0){
					document.systemForm.submit();
				} 
			 });
			 
			 //提交，最终验证。
			 $('#moduleButton').click(function(){
				
				var numError = $('form .onError').length;
				if(numError==0){
					document.moduleForm.submit();
				} 
			 });
			 
			 //提交，最终验证。
			 $('#dbButton').click(function(){
				var numError = $('form .onError').length;
				if(numError==0){
					document.moduleForm.submit();
				} 
			 });
			
		});
</script>


</script>

</head>
<body>
<h4 >&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;参数设置</h4>
<table width="100%" border="0">

  <tr>
    <th width="75%" scope="col">&nbsp;</th>
    <th width="3%" rowspan="2" scope="col" style="BORDER-RIGHT: #d4d4d4 1px dashed;">&nbsp;</th>
    <th width="22%" scope="col">&nbsp;</th>
  </tr>
  <tr>
    <td valign="top">
     
    <!--list start-->
    <table width="650" style="margin-left:100px;">

  <div  id="system" style ="float:2px ,padding:2px">
    <form  id="systemForm" name="systemForm" class ="system" action="/sysMgrServlet?action=system&forwardURL=page/result.jsp&returnURL=javascript:history.back();" method="post">
		系统相关
	<hr style="BORDER-BOTTOM: #d4d4d4 1px dashed; height:2px;">
	
	<div class="int">
		<label>
			当前路径
		</label>
		<input type="text" id="currentPath" name ="currentPath" value="${requestScope.currentPath}"/>
	</div>

	<div class="int">
		<label>
			模板文件路径
		</label>
		<input type="text" name ="templetFilePath" id="templetFilePath" value="${requestScope.templetFilePath}" />
	</div>	

	<div class="int">
		<label>
			相似度匹配大小
		</label>
		<input type="text" name ="fieldMatch"  id ="fieldMatch" value="${requestScope.fieldMatch}"/> 
	</div>	
	
	<div class="int">
		<label>
			端口
		</label>
		<input type="text" name="port" id="port" value="${requestScope.port}" /> 
	</div>	

	<div class="int">
		<label>
			WinRAR路径
		</label>
		<input type="text" name ="winrarPath" value="${requestScope.winrarPath}"/> 
	</div>	

	<div class="int">
		<label>
			最大线程数
		</label>
		<input type="text" name ="maxThreadCount" id ="maxThreadCount" value="${requestScope.maxThreadCount}"/> 
	</div>				
	<div class="int">
		<label>
			最大正常任务线程数
		</label>
		<input type="text" name ="maxCltCount" id ="maxCltCount" value="${requestScope.maxCltCount}"/>
	</div>	

	<div class="int">
		<label>
			最大补采任务线程数
		</label>
		<input type="text" name ="maxRecltCount" id ="maxRecltCount" value="${requestScope.maxRecltCount}"/>
	</div>		
	
    <div class="int">
		<label>
			每次采集最大个数
		</label>
		<input type="text" name ="maxCountPerRegather" id  ="maxCountPerRegather" value="${requestScope.maxCountPerRegather}"/> 
	</div>		  
	
	<div class="int">
		<label>
			版本号
		</label>
		<span>${requestScope.edition}</span>
	</div>	
	
	<div class="int">
		<label>
			版本发布时间
		</label>
		<span>${requestScope.releaseTime}</span>
	</div>	

     <div class="sub" >
		<button type="button" name="systemButton" id="systemButton">
			确定
		</button>

	</div>

	</form>
	
	
	 <form  id="dbForm" name="dbForm" class ="system" action="/sysMgrServlet?action=db&forwardURL=page/result.jsp&returnURL=javascript:history.back();" method="post">
		数据库 
		<hr style="BORDER-BOTTOM: #d4d4d4 1px dashed; height:2px;">
	<div class="int">
		<label>
			类型
		</label>
		<input type="text" name="type" id="type"  value="${requestScope.type}"/>
	</div>	

	<div class="int">
		<label>
			驱动类名
		</label>
		<input type="text" name="driverClassName" value="${requestScope.driverClassName}"/>
	</div>	
	
	<div class="int">
		<label>
			链接地址
		</label>
		<input type="text" name="url" value="${requestScope.url}"/>
	</div>	
	
		<div class="int">
		<label>
			服务名
		</label>
		<input type="text" name="service" value="${requestScope.service}"/>
	</div>	
    <div class="int">
		<label>
			用户名
		</label>
		<input type="text" name="dbname" value="${requestScope.name}"/>
	</div>	
    <div class="int">
		<label>
			密码
		</label>
		<input type="text" name="password" value="${requestScope.password}"/>
	</div>	
	    <div class="int">
		<label>
			最大激活数
		</label>
		<input name="maxActive" id="maxActive" type="text" value="${requestScope.maxActive}"/>
	</div>	
	
     <div class="int">
		<label>
			maxIdle
		</label>
		<input name="maxIdle" id="maxIdle" type="text" value="${requestScope.maxIdle}"/>
	</div>	
	     <div class="int">
		<label>
			maxWait
		</label>
		<input name="maxWait" id="maxWait" type="text" value="${requestScope.maxWait}"/>
	</div>	
	
    <div class="int">
		<label>
			validationQuery
		</label>
		<span>${requestScope.validationQuery}</span>
	</div>	
	
	  <div class="sub">
		<input type="submit" id ="dbButton" name ="dbButton" value="修改"/>
	</div>	
	
   </form>
  
    <form  id="moduleForm"  name="moduleForm" class ="system" action="/sysMgrServlet?action=module&forwardURL=page/result.jsp&returnURL=javascript:history.back();" method="post">
		模块
		<hr style="BORDER-BOTTOM: #d4d4d4 1px dashed; height:2px;">
		
    <div class="int">
		<label>
			web模块是否开启
		</label>
		<select  name="isEnableWeb">
	    <option value="true" ${requestScope.isEnableWeb
			eq true ? "selected='selected'" : "" }>
			开启
		</option>
		<option value="false" ${requestScope.isEnableWeb
			eq false ? "selected='selected'" : "" }>
			关闭
		</option>
     </select>
	</div>		
		
     <div class="int">
		<label>
			字符集
		</label>
			<input name="charset" type="text" value="${requestScope.charset}"/>
	</div>	
			
     <div class="int">
		<label>
			HttpServer类
		</label>
			${requestScope.webServerClass}
	</div>	
	<div class="int">
		<label>
			webapp
		</label>
			${requestScope.webApp}
	</div>
	
    <div class="int">
		<label>
			web上下文路径
		</label>
			${requestScope.webContextPath}
	</div>
	
   <div class="int">
		<label>
			日志级别
		</label>
			 <select  name="loglevel">
       	   <option value="1" ${requestScope.loglevel
			eq 1 ? "selected='selected'" : "" }>
			INFO
		</option>
		<option value="0" ${requestScope.loglevel
			eq 0 ? "selected='selected'" : "" }>
			DEBUG
		</option>
		 <option value="2" ${requestScope.loglevel
			eq 2 ? "selected='selected'" : "" }>
			WARN
		</option>
		<option value="3" ${requestScope.loglevel
			eq 3 ? "selected='selected'" : "" }>
			ERROR
		</option>
	   <option value="4" ${requestScope.loglevel
			eq 4 ? "selected='selected'" : "" }>
			FATAL
		</option>
     </select>
	</div>
	
      <div class="int">
		<label>
			告警模板是否开启
		</label>
		 <select  name="alarmEnable">
	    <option value="true" ${requestScope.alarmEnable
			eq true ? "selected='selected'" : "" }>
			开启
		</option>
		<option value="false" ${requestScope.alarmEnable
			eq false ? "selected='selected'" : "" }>
			关闭
		</option>
     </select>
	</div>
    
    <div class="int">
		<label>
			邮件发送类
		</label>
			${requestScope.senderBean}
	</div>
    
    
      <div class="int">
		<label>
			告警过滤器
		</label>
			${requestScope.filters}
	</div>
	
	 <div class="int">
		<label>
			文件生命周期是否开启
		</label>
			<select  name="dataFileLifecycleEnable">
       	   <option value="true" ${requestScope.dataFileLifecycleEnable
			eq true ? "selected='selected'" : "" }>
			开启
		</option>
		<option value="false" ${requestScope.dataFileLifecycleEnable
			eq false ? "selected='selected'" : "" }>
			关闭
		</option>
     </select>
	</div>
	
    <div class="int">
		<label>
			文件后缀名
		</label>
			<input name="fileExt" type="text"  id="fileExt" value="${requestScope.fileExt}"/>
	</div>
	
	<div class="int">
		<label>
			生命周期
		</label>
			<input name="lifecycle"  id="lifecycle" type="text" value="${requestScope.lifecycle}"/>
	</div>

	<div class="int">
		<label>
			delWhenOff
		</label>
	    <select  name="delWhenOff">
       	   <option value="true" ${requestScope.delWhenOff
			eq true ? "selected='selected'" : "" }>
			开启
		</option>
		<option value="false" ${requestScope.delWhenOff
			eq false ? "selected='selected'" : "" }>
			关闭
		</option>
     </select>
	</div>
		
	  <div class="sub">
		<input type="submit" id="moduleButton" value="修改"/>
		</div>  
    
	</from>                   
</table>
    <!--list end-->
    </td>
    <td style="padding-left:10px;"  valign="top">
      <table width="100%" border="0" style="margin-top:10px;">
        <tr>
          <td><span class="navLink"><a href="page/sysmgr/index.jsp" title="点击返回系统管理主界面">>> 返回 </a></span></td>
        </tr>
    </table>

    </td>
  </tr>
</table>
</body>
</html>