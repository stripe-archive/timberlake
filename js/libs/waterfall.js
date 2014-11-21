(function() {
d3.waterfall = function() {
  var g = d3.select(this);
  var textFormat = function(d) { return d; };
  var linkFormat = null;
  var tickFormat = d3.time.format.utc('%H:%M');
  var width = 1;
  var height = 1;
  var barHeight = 1;
  var domain = [0, 1];
  var barStyle = function(d) { console.log('bbb'); return {}; };

  function waterfall(g) {
    g.each(function(data, i) {


      var x = d3.time.scale.utc().range([0, width]);
      var y = d3.scale.linear().range([height, 0]);

      x.axis = d3.svg.axis().scale(x).orient('bottom').ticks(4).tickFormat(tickFormat);

      x.domain(domain);
      y.domain([0, data.length]);

      g.append('g')
          .attr('class', 'x axis')
          .attr('transform', translate(0, height))
          .call(x.axis);

      var bars = g.selectAll('.bar')
          .data(data)
        .enter().append('g')
          .attr('class', 'bar')
          .attr('transform', function(d, i) { return translate(x(d.start), y(data.length - i)); });

      bars.append('rect')
          .attr('width', function(d) { return x(d.finish) - x(d.start); })
          .attr('height', function(d) { return barHeight; })
          .style('fill', barStyle);

      if (linkFormat) {
        bars.append('svg:a')
            .attr('xlink:href', linkFormat)
          .append('text')
            .text(textFormat)
            .attr('dx', 2)
            .attr('dy', barHeight / 2 + 5);
      } else {
        bars.append('text')
          .text(textFormat)
          .attr('dx', 2)
          .attr('dy', barHeight / 2 + 5);
      }
    });
  }
  waterfall.tickFormat = function(x) {
    if (!arguments.length) return tickFormat;
    tickFormat = x;
    return waterfall;
  };

  waterfall.textFormat = function(x) {
    if (!arguments.length) return textFormat;
    textFormat = x;
    return waterfall;
  };

  waterfall.linkFormat = function(x) {
    if (!arguments.length) return linkFormat;
    linkFormat = x;
    return waterfall;
  };

  waterfall.width = function(x) {
    if (!arguments.length) return width;
    width = x;
    return waterfall;
  };

  waterfall.height = function(x) {
    if (!arguments.length) return height;
    height = x;
    return waterfall;
  };

  waterfall.barHeight = function(x) {
    if (!arguments.length) return barHeight;
    barHeight = x;
    return waterfall;
  };

  waterfall.domain = function(x) {
    if (!arguments.length) return domain;
    domain = x;
    return waterfall;
  };

  waterfall.barStyle = function(x) {
    if (!arguments.length) return barStyle;
    barStyle = x;
    return waterfall;
  };

  return waterfall;
};

function translate(x, y) { return 'translate(' + x + ',' + y + ')'; }

})();
