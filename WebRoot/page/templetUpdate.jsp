<%@ page contentType="text/html; charset=utf-8" language="java" %>
<%@taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">
<script type="text/javascript">
         function getValidate(){
			 var bool= confirm( "确定提交修改吗？");
			 if (!bool){
			  	return false;
			 }
			  
             var tmpType = document.getElementById("tmpType").value;
             var tmpName = document.getElementById("tmpName").value;
             var edition = document.getElementById("edition").value;             
             var tempFileName = document.getElementById("tempFileName").value;

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
                alert("版本号述过长.");
                return false;
             }
             if(tempFileName.length >50){
                alert("模板名称过长.");
                return false;
             }
              
            return true;       
         }       
		 
		 // 在线编辑模板文件按钮点击事件
		 function editTempletFileOnline(){
			 var fileName = "${requestScope.result.data.tempFileName}";
			 if (fileName == null || fileName == "")
			 	return;
			
			 var editURL = "TempletFileMgr.do?id="+fileName+"&action=get&forwardURL=templetFileUpdate.jsp&returnURL=TempletFileMgr.do";
			 window.location.href = editURL;
		 }
</script>

<html>
  <head>  
  <META content=no-cache http-equiv=Pragma> 
  <title>${requestScope.result.data.tempFileName}</title>
  </head>
  <link href="/css/igp.css" rel="stylesheet" type="text/css" />
  <script language="javascript" src="/js/jquery.js" ></script>
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
    <form action="template.do?action=update&id=${requestScope.result.data.tmpID}" method="post" name="form" onSubmit="return getValidate()">
	<table width="460" style="margin-left:50px;" class="thead-left">
		<tr>
		  <td colspan="2" align="center"><span class="addDeviceTitle">模板信息编辑</span></td>
		</tr>
		<c:set var="tem" value="${requestScope.result.data}"></c:set>
        <tr style="">
		  <td colspan="2" style="height:5px;BORDER-TOP: #d4d4d4 1px dashed; padding-bottom:10px;"></td>
		</tr>
        <tr>
			<td width="77" height="30" align="center">模板编号</td>
			<td width="371"><font color="#FF0000">${tem.tmpID}</font></td>
		</tr>

		<tr>
			<td height="30" align="center">模板类型<br></td>
			<td><input type="text" name="tmpType" id="tmpType" value="${tem.tmpType}" style="ime-mode:Disabled" onKeyPress="return event.keyCode>=48&&event.keyCode<=57" maxlength="8" ondragenter="return false" onpaste="return !clipboardData.getData('text').match(/\D/)" />&nbsp;<font color="#999999">必须为数字,必填项</font></td>
		</tr>	
		<tr>
			<td height="30" align="center">模板描述<br></td>
			<td><input type="text" name="tmpName" id="tmpName" value="${tem.tmpName}" maxlength="50" />&nbsp;<font color="#999999">可选项</font></td>
		</tr>
		<tr>
			<td height="30" align="center">版本编号<br></td>
			<td><input type="text" name="edition" id="edition" value="${tem.edition}" maxlength="15" />&nbsp;<font color="#999999">可选项</font></td>
		</tr>		
		<tr>
			<td height="30" align="center">模板文件名<br></td>
			<td><input type="text" name="tempFileName" id="tempFileName" value="${tem.tempFileName}" maxlength="40" />&nbsp;<font color="#999999">必填项</font></td>
		</tr>											
		<tr align="center">
			<td height="40" colspan="2"><input type="submit" value="保存修改" />
  &nbsp;&nbsp;&nbsp;&nbsp;<input name="returnBtn" type="button" value="返回" onClick="window.location.href='template.do';" />&nbsp;&nbsp;&nbsp;&nbsp;<input name="refreshBtn" type="button" value="刷新" onClick="window.location.reload();" />&nbsp;&nbsp;&nbsp;&nbsp;<input name="editBtn" id="editBtn" type="button" value="在线编辑模板文件" onClick="return editTempletFileOnline();" /></td>
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
          <td><span class="navLink"><a href="templetAdd.jsp" title="点击添加模板信息">>> 添加模板 </a></span></td>
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
