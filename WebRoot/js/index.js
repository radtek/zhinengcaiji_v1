$(document).ready(function() {
	$(".menu").find("td").each(function() {
		$(this).mouseover(function() {
			$(this).removeClass("menuOff");
			$(this).addClass("menuOn");
		});
		$(this).mouseout(function() {
			$(this).removeClass("menuOn");
			$(this).addClass("menuOff");
		});
	});
});