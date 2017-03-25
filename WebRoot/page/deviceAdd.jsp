<%@ page language="java" pageEncoding="utf-8"%>
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">
<script type="text/javascript">
 function getValidate(){
	 var deviceId = document.getElementById("deviceId").value;
	 var deviceName = document.getElementById("deviceName").value;
	 var cityId = document.getElementById("cityId").value;
	 var omcId = document.getElementById("omcId").value;
	 
	 var vendor = document.getElementById("vendor").value;
	 var hostIp = document.getElementById("hostIp").value;
	 var userName = document.getElementById("userName").value;
	 var password = document.getElementById("password").value;
	 var hostSign = document.getElementById("hostSign").value;

	  if(deviceId ==""){
		alert("设备编号不能为空.");
		document.getElementById("deviceId").focus();
		return false;
	  }
	   if(isNaN(deviceId)){
		 alert("设备编号必须是数字.");
		 return false;
	  }
	  if(deviceId >999999999){
		alert("设备编号不能大于999999999.");
		return false;
	  }
	  
	  if(deviceName ==""){
		alert("设备名称不能为空.");
		document.getElementById("deviceName").focus();
		return false;
	  }
	  if(deviceName.length > 18){
		alert("设备名称长度超出取值范围.");
		document.getElementById("deviceName").focus();
		return false;
	  }
	  
	  if(cityId ==""){
		alert("城市编号不能为空.");
		document.getElementById("cityId").focus();
		return false;
	  }
	  if(isNaN(cityId)){
		 alert("城市编号必须是数字.");
		 return false;
	  }
	  if(cityId >999999999){
		alert("城市编号不能大于999999999.");
		return false;
	  }
	  
	   if(omcId ==""){
		alert("omc id 不能为空.");
		document.getElementById("omcId").focus();
		return false;
	  }
	  if(isNaN(omcId)){
		 alert("omc id 必须是数字.");
		 return false;
	  }
	  if(omcId >999999999){
		alert("omc id 不能大于999999999.");
		return false;
	  }
		   
	  if(vendor ==""){
		alert("厂商不能为空.");
		document.getElementById("vendor").focus();
		return false;
	  } 
	  if(vendor.length > 6){
		alert("厂商名称长度超出取值范围.");
		document.getElementById("vendor").focus();
		return false;
	  }
	  
	 if(hostIp ==""){
		alert("IP不能为空.");
		document.getElementById("hostIp").focus();
		return false;
	  } 
							
	  if(userName ==""){
		alert("用户名不能为空.");
		document.getElementById("userName").focus();
		return false;
	  } 
	  if(userName.length > 18){
		alert("用户名长度超出取值范围.");
		document.getElementById("userName").focus();
		return false;
	  }  
	  if(password.length > 18){
		alert("密码长度超出取值范围.");
		return false;
	  }
	  if(hostSign.length > 30){
		alert("提示符长度超出取值范围.");
		return false;
	  }    
 	  return checkIP(hostIp);      
	return true;                 		
   }   
         
   //校验IP地址格式   
function checkIP(ip) 
{ 
	//ip地址
	var exp=/^(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])\.(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])\.(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])\.(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])$/; 
	var reg = ip.match(exp); 
	if(reg==null) 
	{ 
	 alert("IP地址不合法！");
	 return false;     
	 
	} 
    return true;         
}  
         
</script>

<html>
  <head> 
  	<META content=no-cache http-equiv=Pragma> 
    <title>添加设备</title>
  <style type="text/css">
<!--
.addDeviceTitle {
	font-size: 15px;
	font-weight: bold;
	float: left;
}
-->
  </style>
  </head>
  <link href="/css/igp.css" rel="stylesheet" type="text/css" />
<script language="javascript" src="/js/jquery.js" ></script>
<script type="text/javascript">
	$(document).ready(function(){    
			$("#deviceId").focus();
			}
	);
</script>
  <style type="text/css">
<!--
.searchBox {
	float: left;
	margin-bottom:5px;
	color:#090;
	font-weight: bold;
}
.thead-left{
	
}
.thead-left TD{
	text-align:left;
}
-->
</style>

 <body>
    <table width="100%" border="0">
  <tr>
    <th width="73%" scope="col">&nbsp;</th>
    <th width="5%" rowspan="2" scope="col" style="BORDER-RIGHT: #d4d4d4 1px dashed;">&nbsp;</th>
    <th width="22%" scope="col">&nbsp;</th>
  </tr>
  <tr>
    <td valign="top">
    <!--left start-->
    <form name="form" action="device.do?action=add" method="post" onSubmit="return getValidate()">
	<table width="470" style="margin-left:50px;" class="thead-left">
		<tr class="thead-left">
		  <td colspan="2"><span class="currentTitle">添加设备</span></td>
		</tr>
        <tr style="">
		  <td colspan="2" style="height:5px;BORDER-TOP: #d4d4d4 1px dashed; padding-bottom:10px;"></td>
		</tr>
		<tr>
			<td width="77" height="30" align="center">设备编号</td>
			<td width="411"><input name="deviceId" type="text" id="deviceId" style="ime-mode:Disabled;color:#F00;" onkeypress="return event.keyCode>=48&&event.keyCode<=57" maxlength="15" ondragenter="return false" onpaste="return !clipboardData.getData('text').match(/\D/)" />&nbsp;<font color="#999999">必须为数字,必填项</font></td>
		</tr>

		<tr>
			<td height="30" align="center">设备名称<br></td>
			<td><input name="deviceName" type="text" id="deviceName" maxlength="18" />&nbsp;<font color="#999999">长度不能超过 18 位,必填项</font></td>
		</tr>	
		<tr>
			<td height="30" align="center">城市编号<br></td>
			<td><input name="cityId" type="text" id="cityId" style="ime-mode:Disabled" onkeypress="return event.keyCode>=48&&event.keyCode<=57" maxlength="10" ondragenter="return false" onpaste="return !clipboardData.getData('text').match(/\D/)" />&nbsp;<font color="#999999">必须为数字,必填项</font></td>
		</tr>
		<tr>
			<td height="30" align="center">OMC ID<br></td>
			<td><input name="omcId" type="text" id="omcId" style="ime-mode:Disabled" onkeypress="return event.keyCode>=48&&event.keyCode<=57" maxlength="10" ondragenter="return false" onpaste="return !clipboardData.getData('text').match(/\D/)" />&nbsp;<font color="#999999">必须为数字,必填项</font></td>
		</tr>		
		<tr>
			<td height="30" align="center">厂商<br></td>
			<td><input name="vendor" type="text" id="vendor" maxlength="6" />&nbsp;<font color="#999999">长度不能超过 6 位,必填项</font></td>
		</tr>	
				<tr>
			<td height="30" align="center">IP地址<br></td>
			<td><input name="hostIp" type="text" id="hostIp" style="ime-mode:Disabled" onkeypress="return event.keyCode>=48&&event.keyCode<=57 || event.keyCode==46" maxlength="18" ondragenter="return false" onpaste="return !clipboardData.getData('text').match(/\D/)" />&nbsp;<font color="#999999">必填项</font></td>
		</tr>	
				<tr>
			<td height="30" align="center">用户名<br></td>
			<td><input name="userName" type="text" id="userName" maxlength="18" />&nbsp;<font color="#999999">长度不能超过 18 位,必填项</font></td>
		</tr>	
			<tr>
			<td height="30" align="center">密码<br></td>
			<td><input name="password" type="text" id="password" maxlength="18" />&nbsp;<font color="#999999">长度不能超过 18 位,可选项</font></td>
		</tr>	
			<tr>
			<td height="30" align="center">提示符<br></td>
			<td><input name="hostSign" type="text" id="hostSign" maxlength="30" />&nbsp;<font color="#999999">长度不能超过 30 位,必填项</font></td>
		</tr>
        <tr><td colspan="2" style="height:5px;BORDER-TOP: #d4d4d4 1px dashed; padding-bottom:10px;"></td></tr>								
		<tr align="center">
			<td height="40">&nbsp;</td>
			<td><input type="submit" value="添加" />
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<input name="returnBtn" type="button" value="返回" onClick="window.location.href='device.do';"/></td>
		</tr>
	</table>
</form>
   
    <!--left end-->
    </td>
    <td valign="top" style="padding-left:10px;">
      <table width="100%" border="0" style="margin-top:30px;">
        <tr>
          <td><span class="navLink"><a href="device.do" title="点击返回厂商设备列表">>> 返回设备列表 </a></span></td>
        </tr>
    </table></td>
  </tr>
</table>
  </body>
</html>
