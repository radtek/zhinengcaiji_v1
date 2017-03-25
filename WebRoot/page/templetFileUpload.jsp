<%@ page contentType="text/html; charset=utf-8" language="java" %>
<%@taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>

<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
<META content=no-cache http-equiv=Pragma>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
<title>上传模板文件</title>
<link href="/css/igp.css" rel="stylesheet" type="text/css" />
<script language="javascript" src="/js/jquery.js" ></script>
<script type="text/javascript">
    function check()
	{
		var fValue1 = document.getElementById("file1").value;
		var fValue2 = document.getElementById("file2").value;
		if ( (fValue1 == null || fValue1 == "" ) && (fValue2 == null || fValue2 == "")){
			alert("请选择上传文件");
			return false;
		}
		return true;
	}
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
</head>

<body>
<table width="100%" border="0">
  <tr>
    <th width="56%" scope="col">&nbsp;</th>
    <th width="17%" rowspan="2" scope="col" style="BORDER-RIGHT: #d4d4d4 1px dashed;">&nbsp;</th>
    <th width="27%" scope="col">&nbsp;</th>
  </tr>
  <tr>
    <td valign="top">
    <!--list start-->
    <table width="100%" style="margin-left:30px;">
    <tr class="thead-left" >
        <td colspan="3" height="40"><span class="currentTitle">上传模板文件</span></td>
    </tr>
    <tr><td colspan="3" style="BORDER-BOTTOM: #d4d4d4 1px dashed; height:2px;"></td></tr>
    <tr><td colspan="3" height="10"></td></tr>
    <form action="TempletFileUpload.do" name="frm" id="frm" enctype="multipart/form-data" method="post" onsubmit="return check();">
	<tr class="thead-left" >
		<td width="11%" height="30">文件一</td>
		<td width="83%"><input name="file1" type="file" id="file1" size="60" /></td>
		<td width="6%">&nbsp;</td>		
		</tr>	
    <tr class="thead-left" >
		<td width="11%" height="30">文件二</td>
		<td width="83%"><input name="file2" type="file" id="file2" size="60" /></td>
		<td>&nbsp;</td>		
		</tr>
    <tr class="thead-left" >
		<td height="50" colspan="3"><input type="submit" name="uploadBtn" id="uploadBtn" value="上传" /></td>
		</tr>
    </form>
    <tr class="thead-left" >
      <td height="20" colspan="3">&nbsp;</td>
    </tr>
    <tr class="thead-left" >
      <td height="50" colspan="3">注意:<br>1.单个文件大小不能超过5M;<br>2.只允许上传XML文件;<br>3.文件将以原文件名上传至采集服务器系统模板位置;<br>4.如果此文件的文件名在采集服务器系统模板目录中有则内容将会被覆盖;<br>5.出于性能考虑,如果文件较多或者文件较大建议直接通过网络复制文件到采集服务器，而放弃使用在线上传工具;</td>
    </tr>
	</table>
    <!--list end-->
    </td>
    <td style="padding-left:10px;" valign=top>
      <form name="form" action="TempletFileMgr.do?action=query" method="post">
        <table width="100%" border="0">
          <tr>
            <th scope="col"><input type="text" name="keyword" id="keyword" class="searchBox" /></th>
          </tr>
          <tr><td height="40"><input type="submit" value="查找文件" /></td></tr>
        </table>
      </form>
      
      <table width="100%" border="0" style="margin-top:30px;">
        <tr>
          <td><span class="navLink"><a href="TempletFileMgr.do" title="点击返回模板文件管理">>> 返回 </a></span></td>
        </tr>
    </table>
    
      <table width="100%" border="0" style="margin-top:30px;">
        <tr>
          <td><span class="navLink"><a href="templetAdd.jsp" title="点击添加模板">>> 添加模板 </a></span></td>
        </tr>
    </table>
    <table width="100%" border="0" style="margin-top:30px;">
        <tr>
          <td><span class="navLink"><a href="template.do" title="点击返回模板列表">>> 返回模板列表 </a></span></td>
        </tr>
    </table>
    </td>
  </tr>
</table>

</body>
</html>
