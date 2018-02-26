import React from 'react';

import {
  COLOUR_MAP,
  COLOUR_SELECTED,
  COLOUR_HOVER,
} from '../utils/utils';
import {secondFormat} from '../utils/d3';
import Waterfall from './Waterfall';

const {_} = window;

export default class extends React.Component {
  render() {
    const {hover, job, relatives} = this.props;

    if (relatives.length < 2) return null;

    let data = relatives.map((j) => ({
      start: j.startTime,
      finish: j.finishTime || new Date(),
      job: j,
    }));
    data = _.sortBy(data, 'start');

    const fmt = (d) => `${secondFormat(d.job.duration())} ${d.job.name}`;
    const links = (d) => `#/job/${d.job.id}`;
    const fs = (d) => {
      if (d.job.id == job.id) { // eslint-disable-line eqeqeq
        return COLOUR_SELECTED;
      } else if (hover && d.job.id == hover.id) { // eslint-disable-line eqeqeq
        return COLOUR_HOVER;
      } else {
        return COLOUR_MAP;
      }
    };

    return (
      <div>
        <h4>Related Jobs</h4>
        <Waterfall
          data={data}
          barHeight={30}
          lineHeight={40}
          width={1200}
          textFormat={fmt}
          fillStyle={fs}
          linkFormat={links}
        />
      </div>
    );
  }
}
