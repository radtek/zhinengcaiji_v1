<%@ page contentType="text/html; charset=utf-8" language="java" import="java.util.*" errorPage="" %>
<%@page import="db.pojo.Vendor"%>
<%@page import="db.dao.VendorDAO"%>
<%
VendorDAO dao = new VendorDAO();
List<Vendor> vendors = dao.list();
%>
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
<title>厂商列表</title>
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
.currentTitle{
	font-size:18px;
	font-weight:bold;
}
-->
</style>
</head>

<body>
<table width="100%" border="0">
  <tr>
    <th width="65%" scope="col">&nbsp;</th>
    <th width="3%" rowspan="2" scope="col" style="BORDER-RIGHT: #d4d4d4 1px dashed;">&nbsp;</th>
    <th width="32%" scope="col">&nbsp;</th>
  </tr>
  <tr>
    <td valign="top">
    <!--list start-->
    <table width="400" style="margin-left:100px;">
    <tr class="thead-left" >
		<td colspan="3" height="60"><span class="currentTitle">厂商列表 <font color="#999999">(只读)</font></span></td>
	</tr>
	<tr class="thead-left" >
		<td width="14%">编号</td>
		<td width="40%">中文名称</td>
		<td width="46%">英文名称</td>
	</tr>
    <tr><td colspan="10" style="BORDER-BOTTOM: #d4d4d4 1px dashed; height:2px;"></td></tr>		 
<%  		
         if(vendors != null && vendors.size() > 0){
              Iterator it = vendors.iterator();        
              while(it.hasNext()){
                    Vendor v = (Vendor)it.next();               
	        	   	out.println("<tr height=25 style=\"cursor:pointer;\"  onMouseOver=\"JavaScript:this.style.background='#ffdfc1'\" onmouseout=\"JavaScript:this.style.background='#ffffff'\"> ");
	        	   	out.println("<td>"+v.getId()+"</td>");
	            	out.println("<td>"+v.getNameCH()+"</td>");
	            	out.println("<td>"+v.getNameEN()+"</td>");
	            	out.println("</tr>"); 		 
               }
         }      
%>
</table>
    <!--list end-->
    </td>
    <td style="padding-left:10px;"  valign="top">
      <table width="100%" border="0" style="margin-top:10px;">
        <tr>
          <td><span class="navLink"><a href="index.jsp" title="点击返回系统管理主界面">>> 返回 </a></span></td>
        </tr>
    </table>
    </td>
  </tr>
</table>
</body>
</html>