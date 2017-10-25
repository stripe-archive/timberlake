import React from 'react';

import BoxPlot from './BoxPlot';
import {
  sample,
  secondFormat,
} from '../utils';

const {_} = window;

export default class extends React.Component {
  render() {
    const tasks = this.props.tasks.filter((t) => !t.bogus);
    const durations = sample(tasks.map((x) => x.duration()).sort(), 100, _.identity);
    return (
      <div>
        <h4>{this.props.title} Timing</h4>
        {durations.length ? <BoxPlot data={durations} tickFormat={secondFormat} /> : null}
      </div>
    );
  }
}
