<%@ page contentType="text/html; charset=utf-8" language="java" %>
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
<title>数据源表达式匹配测试</title>
<link href="/css/igp.css" rel="stylesheet" type="text/css" />
<script language="javascript" src="/js/jquery.js" ></script>
<script language="javascript" charset="gb2312" type="text/javascript" src="/js/DateTimeCalendar.js"></script>
<style type="text/css">
<!--
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
<SCRIPT type=text/javascript>
$(document).ready(function(){    
            $("#expr").focus();
			
			$("#testBtn").click(function(){
			 	var expr = $("#expr").val();
				var time = $("#time").val();
			 	if(expr==""){
			 		alert("请输入表达式.");
					$("#expr").focus();
			 		return;
			 	}
				if(time==""){
			 		alert("请选择时间.");
			 		return;
			 	}
			 	$.post("/ToysServlet",{expr:expr,time:time,action:"dsExprTest"},function(data){
			 		$("#result").val(data);
			 	});
			 });
			
			$("#copytoClipboard").click(function(){
				window.clipboardData.setData('text', $("#result").val()); 
			})
			
			}
);
    
</SCRIPT>
<table width="100%" border="0">
  <tr>
    <th width="69%" scope="col">&nbsp;</th>
    <th width="3%" rowspan="2" scope="col" style="BORDER-RIGHT: #d4d4d4 1px dashed;">&nbsp;</th>
    <th width="28%" scope="col">&nbsp;</th>
  </tr>
  <tr>
    <td valign="top">
    
    <!--form start-->
    <form name="form1" method="post" action="">
    <table width="680" style="margin-left:50px;">
    <tr class="thead-left" >
		<td height="60"><span class="currentTitle">数据源表达式匹配测试</span></td>
	</tr>
	<tr class="thead-left" >
		<td height="30">数据源表达式:</td>
		</tr> 
    <tr class="thead-left" >
      <td><input name="expr" type="text" id="expr" tabindex="1" size="90" /></td>
      </tr>
    <tr class="thead-left" >
      <td height="10"></td>
    </tr>
    <tr class="thead-left" >
		<td height="30">时间:</td>
		</tr>
    <tr class="thead-left" >
      <td><input type="text" name="time" id="time" tabindex="2" onfocus="calendar();" /></td>
      </tr>
    <tr class="thead-left" >
		<td height="30">匹配结果:</td>
		</tr>
    <tr class="thead-left" >
      <td><textarea name="result" id="result" cols="85" rows="5" readonly="readonly" style=" background-color:#EAF4F7"></textarea></td>
      </tr>

    <tr class="thead-left" >
      <td height="40">
      <label>
        <input type="button" name="testBtn" id="testBtn" value="测试">
      </label>
      &nbsp;&nbsp;&nbsp;&nbsp;
      <input type="button" name="copytoClipboard" id="copytoClipboard" value="复制结果到剪贴板">
      <label>&nbsp;&nbsp;&nbsp;&nbsp;
        <input type="reset" name="reset" id="reset" value="重置">
      </label></td>
     </tr>
	</table>
    </form>
    <!--form end-->
    
    </td>
    <td style="padding-left:10px;"  valign="top">
      <table width="100%" border="0" style="margin-top:10px;">
        <tr>
          <td><span class="navLink"><a href="javascript:history.back(-1);" title="点击返回">>> 返回 </a></span></td>
        </tr>
    </table></td>
  </tr>
</table>
</body>
</html>