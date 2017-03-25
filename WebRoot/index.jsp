<%@ page contentType="text/html; charset=utf-8" language="java"%>
<%@page import="util.Util"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
	<head>
		<meta content=no-cache http-equiv=Pragma />
		<meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
		<title>IGP-登陆</title>
		<link href="css/igp.css" rel="stylesheet" type="text/css" />
		<script language="javascript" src="js/jquery.js"></script>
	</head>

	<body>
		<div id=toper>
			<div class="log-img-div">
				<img src="images/igp_logo.jpg" alt="IGP" title="IGP(智能采集平台)" />
			</div>
			<div class="top-nav"></div>
		</div>

		<div id=wrapper>

			<h1 style="padding-left: 50px;">
				登陆
				<font color="#999966"><%=Util.getHostName()%></font>
			</H1>
			<FORM id="form" method=post name=lform action="/auth">
				<DIV style="PADDING-LEFT: 100px">
					<BR>
						账号:<BR>
							<INPUT id=username tabIndex=1 maxLength=25 size=25 type=text
								name="userName" value="igp" />
								<BR>
									<BR>
										密码:<BR>
											<INPUT id=userpwd tabIndex=2 maxLength=20 size=10
												type=password name="password"  value="igp"/>
												<A href="">忘记密码了</A><BR>
													<BR>
														<LABEL class=pl>
															<INPUT tabIndex=3 type=checkbox name="remember">
																&nbsp; 在这台电脑上记住我(一个月之内不用再登录) 
														</LABEL>
														<BR>
															<BR>
																<BR>
																	<INPUT tabIndex=4 value="进入" type=submit
																		name="user_login">
				</DIV>
			</FORM>
			<SCRIPT type=text/javascript>
	$(document).ready(function() {
		if ($("#username").val()) {
			$("#userpwd").focus();
		} else {
			$("#username").focus();
		}
	});
</SCRIPT>

			<br>
				<br>

					<DIV id=footer>
						<SPAN class="fleft gray-link">© 2010－2020 uway.cn, all
							rights reserved </SPAN>
						<SPAN class=fright> <A href="">About IGP</A> · <A href="">Contact
								us</A> · <A href="page/help">Help</A> · <A href="page/api">IGP
								API</A> </SPAN>
					</DIV>
		</div>

	</body>
</html>