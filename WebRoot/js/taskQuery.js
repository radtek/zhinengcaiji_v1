$(document).ready(function() {
	// ∑µªÿ
		$("#back").click(function() {
			window.history.go(-1);
		});
		// ≤È—Ø
		$("#query").click(function() {
			$("form:first").submit();
		});
	});