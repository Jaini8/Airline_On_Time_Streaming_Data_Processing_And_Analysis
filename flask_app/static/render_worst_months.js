function update() {
    jQuery.getJSON(base_url + '/get_data', (data) => {
        console.log(data);

        data.forEach(item => {
            // var name = item.name;
            // series.show();
            var month = item.Month;
            var year = item.Year;
            var value = item.value;
            chart.addData({ 'value': value, 'month': month, 'year': year });
        });
    });
}


var chart = null;
var series = null;
var heatLegend =null;

function handleHover(column) {
    if (!isNaN(column.dataItem.value)) {
        heatLegend.valueAxis.showTooltipAt(column.dataItem.value)
    }
    else {
        heatLegend.valueAxis.hideTooltip();
    }
}

$(document).ready(() => {

    am4core.useTheme(am4themes_animated);
    chart = am4core.create("chartdiv", am4charts.XYChart);
    chart.maskBullets = false;

    var xAxis = chart.xAxes.push(new am4charts.CategoryAxis());
    var yAxis = chart.yAxes.push(new am4charts.CategoryAxis());

    xAxis.dataFields.category = "year";
    yAxis.dataFields.category = "month";

    // xAxis.renderer.grid.template.disabled = true;
    xAxis.renderer.maxGridDistance = 40;

    // yAxis.renderer.grid.template.disabled = true;
    // yAxis.renderer.inversed = true;
    yAxis.renderer.maxGridDistance = 30;


    series = chart.series.push(new am4charts.ColumnSeries());
    series.dataFields.categoryX = "year";
    series.dataFields.categoryY = "month";
    series.dataFields.value = "value";
    series.sequencedInterpolation = true;
    series.defaultState.transitionDuration = 3000;
    series.columns.template.width = am4core.percent(100);
    series.columns.template.height = am4core.percent(100);

    series.heatRules.push({
        target: series.columns.template,
        property: "fill",
        min: am4core.color("#ffffff"),
        max: am4core.color("#692155"),
        minValue: 0.0,
        maxValue: 30.0
    });
    yAxis.sortBySeries = series;

    var columnTemplate = series.columns.template;
    columnTemplate.strokeWidth = 2;
    columnTemplate.strokeOpacity = 1;
    columnTemplate.stroke = am4core.color("#ffffff");
    columnTemplate.tooltipText = "{year}/{month}  {value}% bad flights";

    // heat legend
    heatLegend = chart.bottomAxesContainer.createChild(am4charts.HeatLegend);
    heatLegend.width = am4core.percent(100);
    heatLegend.series = series;
    heatLegend.valueAxis.renderer.labels.template.fontSize = 9;
    heatLegend.valueAxis.renderer.minGridDistance = 30;
    // heatLegend.markerCount = 5;


    // heat legend behavior
    series.columns.template.events.on("over", (event) => {
        handleHover(event.target);
    })

    series.columns.template.events.on("hit", (event) => {
        handleHover(event.target);
    })



    series.columns.template.events.on("out", (event) => {
        heatLegend.valueAxis.hideTooltip();
    })


});