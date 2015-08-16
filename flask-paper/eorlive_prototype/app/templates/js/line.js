{
    title: {
        text: "{{data_source_str}}"
    },
    chart: {
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
