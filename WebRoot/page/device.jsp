<%@ page contentType="text/html; charset=utf-8" language="java" %>
<%@taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<script type="text/javascript">
         function getValidate(){
             var select = document.getElementById("type");
             var index = select.selectedIndex;
             var selectvalue = select.options[index].text;         
             var keyWord = document.getElementById("keyword").value;                      
             if(selectvalue !="请选择" && keyWord ==""){
               alert("请输入关键字.");
               return false;
             }           
             if(selectvalue =="请选择" && keyWord !=""){
               alert("请选择查询条件.");
               return false;
             }  
			 if(selectvalue =="设备编号" && keyWord !=""){
				 if (isNaN(keyWord)){
					 alert("设备编号必须是数字.");
					 return false;
				 }
             } 
			 if(selectvalue =="OMC ID" && keyWord !=""){
				 if (isNaN(keyWord)){
					 alert("OMC ID 必须是数字.");
					 return false;
				 }
             }
             return true;                        		
        } 
		
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
<title>厂商设备管理</title>
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
-->
</style>
<SCRIPT type=text/javascript>
$(document).ready(function(){    
            $("#keyword").focus();			
			}
);
    
</SCRIPT>
</head>

<body>
<table width="100%" border="0">
  <tr>
    <th width="80%" scope="col">&nbsp;</th>
    <th width="2%" rowspan="2" scope="col" style="BORDER-RIGHT: #d4d4d4 1px dashed;">&nbsp;</th>
    <th width="18%" scope="col">&nbsp;</th>
  </tr>
  <tr>
    <td valign="top">
    <!--list start-->
    <table width="100%" style="margin-left:20px;margin-right:10px;">
      <tr class="thead-left" >
        <td colspan="10" height="40"><span class="currentTitle">厂商设备管理</span></td>
      </tr>
      
      <c:if test="${! empty requestScope.result.wparam}" >
      <tr class="thead-left" >
        <td colspan="10" height="40"><span style="font-size:16px; font-weight:bold; color:#999">符合 <font color="#FF0000">${requestScope.result.lparam}</font> 为 <font color="#FF0000">${requestScope.result.wparam}</font>  的记录如下:</span></td>
      </tr>
      </c:if>
      
      <tr class="thead-left">
        <td width="9%">设备编号</td>
        <td width="14%">名称</td>
        <td width="8%">城市</td>
        <td width="9%">OMC ID</td>
        <td width="9%">厂商</td>
        <td width="12%">IP</td>
        <td width="9%">用户名</td>
        <td width="10%">密码</td>
        <td width="11%">提示符</td>
        <td width="9%">操作</td>
      </tr>
      <tr>
        <td colspan="10" style="BORDER-BOTTOM: #d4d4d4 1px dashed; height:2px;"></td>
      </tr>
      <c:set var="count" value="0" />
      <c:forEach items="${requestScope.result.data}" var="x">
      <c:if test="${x!=null}">
      	<tr  height=25 style="cursor:pointer;"  onMouseOver="JavaScript:this.style.background='#ffdfc1'" onMouseOut="JavaScript:this.style.background='#ffffff'"> 
            <td>${x.devID}</td>
            <td>${x.devName}</td>
            <td>${x.cityID}</td>
            <td>${x.omcID}</td>
            <td>${x.vendor}</td>
            <td>${x.hostIP}</td>
            <td>${x.hostUser}</td>
            <td>${x.hostPwd}</td>
            <td>${x.hostSign}</td>
            <td><a href='device.do?id=${x.devID}&action=get&forwardURL=deviceUpdate.jsp&returnURL=device.do'>修改</a>  <a href='device.do?id=${x.devID}&action=del&forwardURL=result.jsp&returnURL=device.do' onclick='return getdelete()'>删除</a></td>
	   </tr> 
       <c:set var="count" value="${pageScope.count+1}" />
       </c:if>
      </c:forEach>
      
      <tr><td colspan=10 style="BORDER-TOP: #d4d4d4 1px dashed; height:2px;">共 <font color='#0000ff'>${pageScope.count}</font> 条</td></tr>
    </table>
    <!--list end-->
    </td>
    <td style="padding-left:10px;" valign=top>
      <form name="form" action="device.do?action=query" method="post" onSubmit="return getValidate()">
        <table width="100%" border="0">
          <tr>
            <th scope="col"><input type="text" name="keyword" id="keyword" class="searchBox" /></th>
          </tr>
          <tr>
            <td>
              <select name="type">
                <option value="">请选择</option>
                <option value="id">设备编号</option>
                <option value="name">设备名称</option>
                <option value="omcid">OMC ID</option>
              </select> &nbsp;   
              <input type="submit" value="查询"/>   
            </td>
          </tr>
          <tr>
            <td></td>
          </tr>
        </table>
      </form>
      <table width="100%" border="0" style="margin-top:30px;">
        <tr>
          <td><span class="navLink"><a href="deviceAdd.jsp" title="点击添加厂商设备">>> 添加设备 </a></span></td>
        </tr>
    </table>
    </td>
  </tr>
</table>

</body>
</html>
