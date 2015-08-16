{
    title: {
        text: "{{data_source_str}}"
    },
    chart: {
        type: "column",
        zoomType: "x",
        events: {
            selection: function(event) {
                if (clickDragMode === 'flag') {
                    event.preventDefault();
                    flagClickAndDraggedRange(event);
                }
            }
        }
    },
    credits: {
        enabled: false
    },
    xAxis: {
        type: 'datetime'
    },
    legend: {
        enabled: true
    },
    plotOptions: {
        column: {
            dataGrouping: {
                approximation: "average",
                groupPixelWidth: 80,
                forced: true
            },
            pointPlacement: "between",
            groupPadding: 0.1,
            pointPadding: 0
        }
    },
    series: [
    {% if is_set %}
        {% for series in graph_data['series_dict'] %}
        {
            name: "{{series}}",
            data: {{series}}
        },
        {% endfor %}
    {% else %}
        {% for series in graph_data.h0 %}
        {
            name: "{{series}}",
            data: graph_data.{{series}}_h0
        },
        {% endfor %}
    {% endif %}
    ]
}
