function displayResults(tag,data) {
	var widthPerTick = 60
	
	var colorScale = d3.scale.category20().domain(tasks); 
	//var ticks = Array(totalTicks).fill().map((v,i)=>i);
	
	var tickTime = d3.time.minutes
	var endTime = lastTimestamp(data)
	var totalTicks = endTime
	//var totalTicks = tickTime(0,endTime).length	
	console.log("Total Ticks: " + totalTicks)
	
	var chart = d3.timeline()
		.tickFormat( //
				{format: d3.format("03d"),
				tickInterval: 1, // This forces us to start from 001 
				numTicks: totalTicks,
				//tickValues: ticks, // Use this to start from 000
				tickSize: 10,
				})
		/*.tickFormat( //
				{format: d3.time.format("%H"),
				tickTime: tickTime,
				tickInterval: 1,
				tickSize: 10,
				})*/
		.stack()
		.margin({left:100, right:30, top:0, bottom:0})
		.colors( colorScale )
		.colorProperty('task')
		.width(totalTicks*widthPerTick);
	chart.showTimeAxisTick();
	
	//chart.relativeTime();
	//chart.rowSeparators("#555555");

	var backgroundColor = "#eeeeee";
	var altBackgroundColor = "white";
	chart.background(function (datum, i) {
		var odd = (i % 2) === 0;
		return odd ? altBackgroundColor : backgroundColor;
	});
	chart.fullLengthBackgrounds();

	var div = d3.select("body").append("div")	
		.attr("class", "tooltip")				
		.style("opacity", 0);
	
	chart.mouseover(function (d, i, datum) {
		// d is the current rendering object
		// i is the index during d3 rendering
		// datum is the data object
		div.style("left", (d3.event.pageX) + "px")		
           .style("top", (d3.event.pageY - 28) + "px");
		div.text(d.task + "\n" +
				 chart.tickFormat().format(new Date(d.starting_time)) + "-" + chart.tickFormat().format(new Date(d.ending_time)) + "\n" +
				 "Delay: " + chart.tickFormat().format(new Date(d.delay)) + "\n" +
				 "Cost: " + d.cost
		);
		div.transition()		
        	.duration(200)		
        	.style("opacity", .9);		
	});
	chart.mouseout(function (d, i, datum) {
		div.transition()		
        	.duration(500)		
        	.style("opacity", 0);	
	});
	
	var svg = d3.select(tag).append("svg")
		.attr("width", '100%')
		.datum(data).call(chart);
}

function lastTimestamp(data) {
	var result = 0
	for (var i = 0; i < data.length; i++) {
		for (var j = 0; j < data[i].times.length; j++) {
		    if (data[i].times[j].ending_time > result)
		    		result = data[i].times[j].ending_time;
		}
	}
	return result + 1
}

displayResults("#resources",resourceData);
displayResults("#workflows",workflowData);
