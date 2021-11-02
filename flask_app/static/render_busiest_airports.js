function update() {
    jQuery.getJSON(base_url + '/get_data', (data) => {


        // check to make sure data is not empty
        if (data.length > 0) {
            console.log("data:----", data);
            // chart.data = data;
            // chart.invalidateData();
            nextYear(data);
        }
        // nextYear(data);

        // data.forEach(item => {
        //     // var name = item.name;
        //     // series.show();
        //     var airport = item.airport;
        //     var value = item.value;
        //     // var month = item.Month;
        //     var year = item.Year;

        //     // chart.addData({ 'airport': airport, 'value': value, 'year': year });
        //     // chart.invalidateData();
        // });
    });
}

var chart = null;
var series = null;
// var heatLegend = null;
var categoryAxis = null;
var valueAxis = null;
var label = null;
var stepDuration = 4000;

// function handleHover(column) {
//     if (!isNaN(column.dataItem.value)) {
//         heatLegend.valueAxis.showTooltipAt(column.dataItem.value)
//     }
//     else {
//         heatLegend.valueAxis.hideTooltip();
//     }
// }

function nextYear(newData) {
    year = newData[0].Year;

    // if (year > 2008) {
    //     year = 1987;
    // }

    var itemsWithNonZero = 0;;
    if (year == 1987) {
        chart.data = newData;
    } else {
        for (var i = 0; i < newData.length; i++) {
            chart.data[i].value = newData[i].value;
            chart.data[i].airport= newData[i].airport;
            if (newData[i].value > 0) {
                itemsWithNonZero++;
            }
        }
    }
    chart.invalidateData();

    // var newData = allData[year];
    
    // for (var i = 0; i < newData.length; i++) {
    //     // chart.data[i].value = newData[i].value;
    //     if (newData[i].value > 0) {
    //         itemsWithNonZero++;
    //     }
    // }

    

    if (year == 1987) {
        series.interpolationDuration = stepDuration / 4;
        valueAxis.rangeChangeDuration = stepDuration / 4;
    }
    else {
        series.interpolationDuration = stepDuration;
        valueAxis.rangeChangeDuration = stepDuration;
    }

    
    
    label.text = year.toString();

    categoryAxis.zoom({ start: 0, end: itemsWithNonZero / categoryAxis.dataItems.length });
}


$(document).ready(() => {

    // Themes begin
    am4core.useTheme(am4themes_animated);
    // Themes end

    chart = am4core.create("chartdiv", am4charts.XYChart);
    chart.padding(40, 40, 40, 40);

    chart.numberFormatter.bigNumberPrefixes = [
        { "number": 1e+3, "suffix": "K" },
        { "number": 1e+6, "suffix": "M" },
        { "number": 1e+9, "suffix": "B" }
    ];

    label = chart.plotContainer.createChild(am4core.Label);
    label.x = am4core.percent(97);
    label.y = am4core.percent(95);
    label.horizontalCenter = "right";
    label.verticalCenter = "middle";
    label.dx = -15;
    label.fontSize = 50;

    // var playButton = chart.plotContainer.createChild(am4core.PlayButton);
    // playButton.x = am4core.percent(97);
    // playButton.y = am4core.percent(95);
    // playButton.dy = -2;
    // playButton.verticalCenter = "middle";
    // playButton.events.on("toggled", function (event) {
    //     if (event.target.isActive) {
    //         play();
    //     }
    //     else {
    //         stop();
    //     }
    // })

    

    categoryAxis = chart.yAxes.push(new am4charts.CategoryAxis());
    categoryAxis.renderer.grid.template.location = 0;
    categoryAxis.dataFields.category = "airport";
    categoryAxis.renderer.minGridDistance = 1;
    categoryAxis.renderer.inversed = true;
    categoryAxis.renderer.grid.template.disabled = true;

    valueAxis = chart.xAxes.push(new am4charts.ValueAxis());
    valueAxis.min = 0;
    valueAxis.rangeChangeEasing = am4core.ease.linear;
    valueAxis.rangeChangeDuration = stepDuration;
    valueAxis.extraMax = 0.1;

    series = chart.series.push(new am4charts.ColumnSeries());
    series.dataFields.categoryY = "airport";
    series.dataFields.valueX = "value";
    series.tooltipText = "{valueX.value}"
    series.columns.template.strokeOpacity = 0;
    series.columns.template.column.cornerRadiusBottomRight = 5;
    series.columns.template.column.cornerRadiusTopRight = 5;
    series.interpolationDuration = stepDuration;
    series.interpolationEasing = am4core.ease.linear;

    var labelBullet = series.bullets.push(new am4charts.LabelBullet())
    labelBullet.label.horizontalCenter = "right";
    labelBullet.label.text = "{values.valueX.workingValue.formatNumber('#.0as')}";
    labelBullet.label.textAlign = "end";
    labelBullet.label.dx = -10;

    chart.zoomOutButton.disabled = true;

    // as by default columns of the same series are of the same color, we add adapter which takes colors from chart.colors color set
    series.columns.template.adapter.add("fill", function (fill, target) {
        return chart.colors.getIndex(target.dataItem.index);
    });

    // var year = 1987;
    // label.text = year.toString();

    // var interval;

    // function play() {
    //     interval = setInterval(function () {
    //         nextYear();
    //     }, stepDuration)
    //     nextYear();
    // }

    // function stop() {
    //     if (interval) {
    //         clearInterval(interval);
    //     }
    // }




    categoryAxis.sortBySeries = series;



    // chart.data = JSON.parse(JSON.stringify(allData[year]));
    // categoryAxis.zoom({ start: 0, end: 1 / chart.data.length });

    // series.events.on("inited", function () {
    //     setTimeout(function () {
    //         playButton.isActive = true; // this starts interval
    //     }, 2000)
    // })

});