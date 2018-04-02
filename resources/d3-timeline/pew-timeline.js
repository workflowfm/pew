function displayResults(tag,data) {
	var colorScale = d3.scale.category20().domain(tasks) 
	var chart = d3.timeline()
		.tickFormat( //
				{format: d3.time.format("%H"),
				tickTime: d3.time.hours,
				tickInterval: 1,
				tickSize: 15})
		.stack()
		.margin({left:70, right:30, top:0, bottom:0})
		.colors( colorScale )
		.colorProperty('task');
	chart.showTimeAxisTick();
	//chart.rowSeparators("#555555");

	var backgroundColor = "#eeeeee";
	var altBackgroundColor = "white";
	chart.background(function (datum, i) {
		var odd = (i % 2) === 0;
		return odd ? altBackgroundColor : backgroundColor;
	});
	chart.fullLengthBackgrounds();

	var svg = d3.select(tag).append("svg").attr("width", totalTicks*widthPerTick)
		.datum(data).call(chart);
}
displayResults("#resources",resourceData);
displayResults("#simulations",simulationData);
