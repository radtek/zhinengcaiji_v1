<%@ page contentType="text/html; charset=utf-8" language="java"%>
<%@taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<%@taglib prefix="fmt" uri="http://java.sun.com/jsp/jstl/fmt"%>
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">


<html>
	<head>
		<META content=no-cache http-equiv=Pragma>
		<title>${requestScope.result.data.name}</title>
	</head>
	<link href="/css/igp.css" rel="stylesheet" type="text/css" />
	<script language="javascript" src="/js/jquery.js"></script>
	<script type="text/javascript">
         function getValidate(){
			 
			 <c:if test="${requestScope.result.wparam}">
			 return false;
			 </c:if>
			 
			 var bool= confirm( "确定提交修改吗？");
			 if (!bool){
			  	return false;
			 }
              
             return true;       
         }   
		 
		function validateXML()
        {
            var docName = document.getElementById("content").innerText; 
            var xmlDoc = new ActiveXObject("msxml2.DOMDocument");
		    xmlDoc.async = false; 
            var message = "";
            if(xmlDoc.readyState == 4)
            {
                xmlDoc.loadXML(docName);
                //message += "XML DOM parse status: " + xmlDoc.readyState + "<br>";
                if(xmlDoc.parseError.errorCode == 0)
                {
                    message += "Success!";
					document.getElementById("result").style.color = "green";
                }
                else
                {
                    message += "errorCode: " + xmlDoc.parseError.errorCode + "<br>";
                    message += "errorLine: " + xmlDoc.parseError.line + "<br>";
                    message += "errorContent: " + xmlDoc.parseError.srcText + "<br>";
                    message += "errorReason: " + xmlDoc.parseError.reason + "<br>";
					document.getElementById("result").style.color = "red";
                }
                
                document.getElementById("result").innerHTML = message;
            }
        }
		 
		 $(document).ready(function(){
				$("#plus").click(function(){
						$("#content").attr("rows",parseInt($("#content").attr("rows"))+10);
				});
				$("#decrease").click(function(){
						if (parseInt($("#content").attr("rows")) <= 18)
							return;
						$("#content").attr("rows",parseInt($("#content").attr("rows"))-10);
				});
				
				//重命名按钮
				var oldFileName = "${requestScope.result.data.name}";
				$("#renameBtn").click(function(){
						var newFileName = $("#renameInput").val();
						if( newFileName == "" || newFileName == null ){
							$("#renameResult").html("<font color=red>文件名不能为空</font>");
							return;
						}
							
						$.ajaxSetup({async: false});
						$.post("/page/TempletFileMgr.do",{action:"renameAjax",id:oldFileName,newName:newFileName},function(data){
							var tip = data;
							if (tip.indexOf("操作成功") < 0)
							{
								$("#renameResult").html("<font color=red>"+tip+"</font>");
								return;
							}
							window.location.href="TempletFileMgr.do?id="+newFileName+"&action=get&forwardURL=templetFileUpdate.jsp&returnURL=TempletFileMgr.do";
						}
						);
				});
				
				$("#downloadBtn").click(function(){		
					window.open("/page/TempletFileDownload.do?id="+oldFileName);
				});
				
				if (oldFileName == null || oldFileName == ""){
					$("#fileTitle").html("<font color=red>错误: 文件不存在,打开失败!</font>");
				}
				
				//加载模板内容
				var n = "${requestScope.result.data.name}";
				if(n!=""){
					$.ajaxSetup({async: false});
					$.post("/page/TempletFileMgr.do",{action:"getContentByAjax",name:n},function(data){
						$("#content").text(data);
					});
				}
		});
</script>
	<style type="text/css">
<!--
.searchBox {
	float: left;
	margin-bottom: 5px;
	color: #090;
	font-weight: bold;
}

.thead-left {
	
}

.thead-left TD {
	text-align: left;
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
				<th width="94%" scope="col">&nbsp;
					
				</th>
			</tr>
			<tr>
				<td valign="top">
					<!--left start-->
					<form
						action="TempletFileMgr.do?action=update&id=${requestScope.result.data.name}"
						method="post" name="form" onSubmit="return getValidate()">
						<table width="100%" style="margin-left: 50px;" class="thead-left">
							<tr>
								<td colspan="2" align="center">
									<span class="addDeviceTitle" id="fileTitle">${requestScope.result.data.name}</span>
								</td>
							</tr>

							<tr>
								<td height="20" colspan="2" align="center">
									<font color="#999999"><fmt:formatNumber
											value="${requestScope.result.data.size / 1024}" type="number"
											maxFractionDigits="2" /> KB &nbsp;&nbsp;&nbsp;&nbsp;
										${requestScope.result.data.modifyDate}</font>
								</td>
							</tr>

							<tr align="center">
								<td colspan="2">
									<textarea name="content" cols="130" rows="18"
										<c:if test="${requestScope.result.wparam}">readonly="readonly"</c:if>
										id="content"></textarea>
								</td>
							</tr>
							<tr>
								<td width="452" height="40">
									<font color="#999999">如文件内容过多则建议把文件下载到本地进行编辑,之后再上传到采集机上!</font>
								</td>
								<td width="356" height="40">
									<span style="float: right; margin-right: 35px;"><a
										href="#" id="decrease" title="点击缩小内容区域"><font size="+3">-</font>
									</a>&nbsp;&nbsp;&nbsp;<a href="#" id="plus" title="点击放大内容区域"><font
											size="+3">+</font> </a> </span>
								</td>
							</tr>
							<tr align="center">
								<td height="40" colspan="2">
									<c:if test="${!requestScope.result.wparam}">
										<input type="submit" value="保存修改" />
									</c:if>
									&nbsp;&nbsp;&nbsp;&nbsp;
									<input name="returnBtn" type="button" value="返回"
										onClick="window.location.href='TempletFileMgr.do';" />
									&nbsp;&nbsp;&nbsp;&nbsp;
									<input name="refreshBtn" type="button" value="刷新"
										onClick="window.location.reload();" />
									&nbsp;&nbsp;&nbsp;&nbsp;
									<input name="validateXMLBtn" id="validateXMLBtn" type="button"
										value="验证XML模板" onClick="validateXML();" />
                                    &nbsp;&nbsp;&nbsp;&nbsp;
									<input name="renameBtnShow" id="renameBtnShow" type="button"
										value="重命名" onClick="javascript:boxs(1);" />
                                    &nbsp;&nbsp;&nbsp;&nbsp;
									<input name="downloadBtn" id="downloadBtn" type="button"
										value="下载此文件" />
								</td>
							</tr>
							<tr align="center">
								<td colspan="2" height="0">
									<span id="result"></span>
								</td>
							</tr>
						</table>
					</form>

					<!--left end-->
				</td>
			</tr>
		</table>
        
        
<!--提示框代码-->   
<input type="button" id="renameBtn" style="visibility:hidden" />     
<style>
*{ padding:0; margin:0; font-size:12px;}
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
  boht.innerHTML = '<div id="bg"></div><div id="info"><div id="center"><strong>请输入新文件名：</strong><p><input type="text" name="renameInput" id="renameInput" maxlength=40 /> <span id="renameResult"></span></p> <p><a href="javascript:document.getElementById(\'renameBtn\').click();" >确定</a> <a href="javascript:boxs(0);" style="margin-left:40px;">关闭</a></p></div></div>';
  document.getElementById('renameInput').focus();
 }
} 
</script>
<!--提示框代码结束--> 

	</body>
</html>
