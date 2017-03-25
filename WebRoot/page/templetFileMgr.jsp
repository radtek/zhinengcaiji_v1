<%@ page contentType="text/html; charset=utf-8" language="java" %>
<%@taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<%@taglib prefix="fmt" uri="http://java.sun.com/jsp/jstl/fmt"%>
<script type="text/javascript">
        function getdelete(){
              var bool= confirm( "确定要删除吗？"); 
             return bool;
        } 
</script>

<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
<META content=no-cache http-equiv=Pragma>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
<title>模板管理</title>
<link href="/css/igp.css" rel="stylesheet" type="text/css" />
<script language="javascript" src="/js/jquery.js" ></script>
<SCRIPT type=text/javascript>
$(document).ready(function(){    
            $("#keyword").focus();			
			
			//新建文件按钮
			$("#newFileBtn").click(function(){
						var newFileName = $("#fileNameInput").val();
						if( newFileName == "" || newFileName == null )
						{
							$("#tipResult").html("<font color=red>文件名不能为空</font>");
							return;
						}
							
						$.ajaxSetup({async: false});
						$.post("/page/TempletFileMgr.do",{action:"newFileAjax",id:newFileName},function(data){
							var tip = data;
							if (tip.indexOf("操作成功") < 0)
							{
								$("#tipResult").html("<font color=red>"+tip+"</font>");
								return;
							}
							window.location.href="TempletFileMgr.do?action=query&keyword=" + newFileName;
						}
						);
			});
});
    
</SCRIPT>
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
    <th width="76%" scope="col">&nbsp;</th>
    <th width="4%" rowspan="2" scope="col" style="BORDER-RIGHT: #d4d4d4 1px dashed;">&nbsp;</th>
    <th width="20%" scope="col">&nbsp;</th>
  </tr>
  <tr>
    <td valign="top">
    <!--list start-->
    <table width="100%" style="margin-left:30px;">
    <tr class="thead-left" >
        <td colspan="5" height="20"><span class="currentTitle">模板文件管理 <font color="#99CC33">${requestScope.result.lparam}</font></span></td>
    </tr>
    
    <tr class="thead-left" >
        <td colspan="5" height="40"><input name="refreshBtn" type="button" value="刷新" onClick="javascript:window.location.reload();" /> <input name="newFileNameShowBtn" type="button" value="新建文件" style="margin-left:10px;" onClick="javascript:boxs(1);" /></td>
    </tr>
    
    <c:if test="${! empty requestScope.result.wparam}" >
      <tr class="thead-left" >
        <td colspan="5" height="40"><span style="font-size:16px; font-weight:bold; color:#999">符合 <font color="#FF0000">${requestScope.result.wparam}</font> 的文件如下:</span></td>
      </tr>
    </c:if>
    
	<tr class="thead-left" >
		<td width="5%">序号</td>
		<td width="39%">文件名</td>
		<td width="17%">大小 (单位:KB)</td>		
		<td width="17%">修改日期</td>
		<td width="22%">操作</td>
	</tr>
    <tr><td colspan="5" style="BORDER-BOTTOM: #d4d4d4 1px dashed; height:2px;"></td></tr>		 
      <c:set var="count" value="0" />
      <c:forEach items="${requestScope.result.data}" var="x">
      <c:if test="${x!=null}">
      	<tr  height=25 style="cursor:pointer;"  onMouseOver="JavaScript:this.style.background='#ffdfc1'" onMouseOut="JavaScript:this.style.background='#ffffff'"> 
            <td>${pageScope.count+1}</td>
            <td>${x.name}</td>
            <td><fmt:formatNumber value="${x.size / 1024}" type="number" maxFractionDigits="2" /></td>
            <td>${x.modifyDate}</td>
            <td width="25%"><a href='TempletFileMgr.do?id=${x.name}&action=get&forwardURL=templetFileUpdate.jsp&returnURL=TempletFileMgr.do'>编辑</a>  <a href='TempletFileMgr.do?id=${x.name}&action=del&forwardURL=result.jsp&returnURL=TempletFileMgr.do' onclick='return getdelete()'>删除</a>  <a href='TempletFileDownload.do?id=${x.name}'>下载</a></td>
	   </tr> 
       <c:set var="count" value="${pageScope.count+1}" />
       </c:if>
      </c:forEach>
      
      <tr><td colspan=5 style="BORDER-TOP: #d4d4d4 1px dashed; height:2px;">共 <font color='#0000ff'>${pageScope.count}</font> 条</td></tr>
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
          <td><span class="navLink"><a href="templetFileUpload.jsp" title="点击上传模板文件">>> 上传模板文件 </a></span></td>
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

<!--提示框代码-->   
<input type="button" id="newFileBtn" style="visibility:hidden" />     
<style>
*{ padding:0; margin:0;}
#bg{background:#000000;opacity: 0.5;-moz-opacity:0.5; filter:alpha(opacity=50); width:100%; height:100%;position:absolute; top:0; left:0}
#info{height:0px; width:0px;top:50%; left:50%;position:absolute;  line-height:1.7}
#center{background:#fff;border:1px solid #217AC1; width:300px; height:100px; position:absolute; margin:-50px -150px;}
#center strong{ display:block; padding:2px 5px; background:#EBF4FC; color:#519FEE;}
#center p{padding:10px; text-align:center; color:#1C6FB8;}
</style>
<span id="boxs"></span>
<script>
function boxs(v){
 window.scrollTo(0,0);
 var bo = document.getElementsByTagName('body')[0];
 var ht = document.getElementsByTagName('html')[0];
 var boht = document.getElementById('boxs');    
 boht.innerHTML = '';
 bo.style.height='auto';
 bo.style.overflow='auto';
 ht.style.height='auto'; 
 if(v == 1){   
  bo.style.height='100%';
  bo.style.overflow='hidden';
  ht.style.height='100%';  
  boht.innerHTML = '<div id="bg"></div><div id="info"><div id="center"><strong>[新建文件] - 请输入文件名：</strong><p><input type="text" name="fileNameInput" id="fileNameInput" maxlength=40 /> <span id="tipResult"></span></p> <p><a href="javascript:document.getElementById(\'newFileBtn\').click();" >确定</a> <a href="javascript:boxs(0);" style="margin-left:40px;">关闭</a></p></div></div>';
  document.getElementById('fileNameInput').focus();
 }
} 
</script>
<!--提示框代码结束--> 

</body>
</html>
