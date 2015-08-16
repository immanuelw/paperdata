var sliderChanged = function(slider) {
    var newOptions = {
        dataGrouping: {
            groupPixelWidth: slider.value
        }
    };

    for (var i = 0; i < _chart.series.length; ++i) {
        if (i < _chart.series.length - 1) {
            _chart.series[i].update(newOptions, false); // Don't redraw chart.
        } else {
            _chart.series[i].update(newOptions); // Redraw chart.
        }
    }
};
