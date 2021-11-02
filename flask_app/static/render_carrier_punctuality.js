// function start() {
//     // jQuery.get(base_url + '/start', (data) => {
//     //     setInterval(update, 2000);
//     // });

//     let query_name = $('#query')[0].value;
//     console.log(query_name);
//     jQuery.get(base_url + '/start', {'query_name': query_name}, (data) => {
//         console.log(data);
//     })
// }

function update() {
    jQuery.getJSON(base_url + '/get_data', (data) => {
        console.log(data);

        data.forEach(item => {
            var name = item.name;
            // var month = item.Month;
            var year = item.Year;
            var value = item.value;
            var date = new Date(year, 12, 31);

            var series = null;
            if (name in series_map) {
                series = series_map[name];
            } else {
                var series = chart.series.push(new am4charts.LineSeries());
                series.dataFields.dateX = "date";
                series.dataFields.valueY = "value";
                series.strokeWidth = 1.5;
                series.tensionX = 0.8;
                series.tensionY = 0.8;
                

                var bullet = series.bullets.push(new am4charts.CircleBullet());
                bullet.circle.strokeWidth = 1.5;
                bullet.tooltipText = "{name}";

                series.name = name;
                series_map[name] = series;
            }
            series.addData({ 'date': date, 'value': value, 'name': name });
        });
    });
}


var chart = null;
var series_map = {};

$(document).ready(() => {

    am4core.useTheme(am4themes_animated);
    chart = am4core.create("chartdiv", am4charts.XYChart);
    chart.legend = new am4charts.Legend();

    var xAxis = chart.xAxes.push(new am4charts.DateAxis());
    xAxis.title.text = "Time";

    var yAxis = chart.yAxes.push(new am4charts.ValueAxis());
    yAxis.title.text = "Punctuality";

});