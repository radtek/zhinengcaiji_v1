<%@ page contentType="text/html; charset=utf-8" language="java" %>
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">
<script type="text/javascript">
         function getValidate(){
             var tmpID = document.getElementById("tmpID").value;
             var tmpType = document.getElementById("tmpType").value;
             var tmpName = document.getElementById("tmpName").value;
             var edition = document.getElementById("edition").value;             
             var tempFileName = document.getElementById("tempFileName").value;

              if(tmpID ==""){
                alert("模板编号不能为空.");
				document.getElementById("tmpID").focus();
                return false;
              }
              if(tmpID >999999999 ||tmpID < 0){
                alert("模板编号取值范围【0-999999999】");
                return false;
              }

              if(tmpType ==""){
                alert("模板类型不能为空.");
				document.getElementById("tmpType").focus();
                return false;
              }
              if(tmpType >999999999 || tmpType < 0){
                alert("模板类型取值范围【0-999999999】");
                return false;
              }

              if(tmpName.length >40){
                alert("模板描述过长.");
				document.getElementById("tmpName").focus();
                return false;
              }
			  
              if(edition.length >20){
                alert("版本号过长.");
                return false;
              }
			  
              if(tempFileName.length >50){
                alert("模板名称过长.");
                return false;
              }
              
              return true;                 		
     }   
                
</script>

<html>
  <head>  
    <title>增加设备</title>
  </head>
  <link href="/css/igp.css" rel="stylesheet" type="text/css" />
  <script language="javascript" src="/js/jquery.js" ></script>
  <script type="text/javascript">
	$(document).ready(function(){    
			$("#tmpID").focus();
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
.addDeviceTitle {
	font-size: 15px;
	font-weight: bold;
	float: left;
}
-->
</style>


 <body>
    <table width="100%" border="0">
  <tr>
    <th width="65%" scope="col">&nbsp;</th>
    <th width="3%" rowspan="2" scope="col" style="BORDER-RIGHT: #d4d4d4 1px dashed;">&nbsp;</th>
    <th width="32%" scope="col">&nbsp;</th>
  </tr>
  <tr>
    <td valign="top">
    <!--left start-->
    <form name="form" action="template.do?action=add" method="post" onSubmit="return getValidate()">
	<table width="460" style="margin-left:50px;" class="thead-left">
		<tr>
		  <td colspan="2" align="center"><span class="addDeviceTitle">添加模板</span></td>
		</tr>
        <tr style="">
		  <td colspan="2" style="height:5px;BORDER-TOP: #d4d4d4 1px dashed; padding-bottom:10px;"></td>
		</tr>
		<tr>
			<td height="30" align="center">模板编号</td>
			<td><input type="text" name="tmpID" id="tmpID" style="ime-mode:Disabled;color:#F00;" onKeyPress="return event.keyCode>=48&&event.keyCode<=57" maxlength="15" ondragenter="return false" onpaste="return !clipboardData.getData('text').match(/\D/)" />&nbsp;<font color="#999999">必须为数字,必填项</font></td>
		</tr>

		<tr>
			<td height="30" align="center">模板类型<br></td>
			<td><input type="text" name="tmpType" id="tmpType" style="ime-mode:Disabled" onKeyPress="return event.keyCode>=48&&event.keyCode<=57" maxlength="8" ondragenter="return false" onpaste="return !clipboardData.getData('text').match(/\D/)" />&nbsp;<font color="#999999">必须为数字,必填项</font></td>
		</tr>	
		<tr>
			<td height="30" align="center">模板描述<br></td>
			<td><input name="tmpName" type="text" id="tmpName" maxlength="50" />&nbsp;<font color="#999999">可选项</font></td>
		</tr>
		<tr>
			<td height="30" align="center">版本编号<br></td>
			<td><input name="edition" type="text" id="edition" maxlength="15" />&nbsp;<font color="#999999">可选项</font></td>
		</tr>		
		<tr>
			<td height="30" align="center">模板文件名<br></td>
			<td><input name="tempFileName" type="text" id="tempFileName" maxlength="40" />&nbsp;<font color="#999999">必填项</font></td>
		</tr>								
		<tr align="center">
			<td height="40">&nbsp;</td>
			<td><input type="submit" value="添加" />&nbsp;
&nbsp;<input name="returnBtn" type="button" value="返回" onClick="window.location.href='template.do';" /></td>
		</tr>
	</table>
</form>
   
    <!--left end-->
    </td>
    <td valign="top" style="padding-left:10px;">
      <table width="100%" border="0" style="margin-top:30px;">
        <tr>
          <td><span class="navLink"><a href="template.do" title="点击返回模板列表">>> 返回模板列表 </a></span></td>
        </tr>
    </table>
    
    <table width="100%" border="0" style="margin-top:30px;">
        <tr>
          <td><span class="navLink"><a href="TempletFileMgr.do" title="点击管理采集机上的模板文件">>> 模板文件管理 </a></span></td>
        </tr>
    </table>
    
    <table width="100%" border="0" style="margin-top:30px;">
        <tr>
          <td><span class="navLink"><a href="templetFileUpload.jsp" title="点击上传模块文件">>> 上传模板文件 </a></span></td>
        </tr>
    </table>
    </td>
  </tr>
</table>
  </body>
</html>
