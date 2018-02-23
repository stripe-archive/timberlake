import React from 'react';
import Waterfall from './Waterfall';
import {sample} from '../utils';

const {_} = window;

export default class extends React.Component {
  render() {
    let {tasks} = this.props;
    tasks.maps.forEach((d) => { d.type = 'map'; });
    tasks = tasks.maps.concat(tasks.reduces).filter((t) => !t.bogus).map((t) => {
      return {start: t.startTime, finish: t.finishTime || new Date(), type: t.type};
    });
    const data = sample(_.sortBy(tasks, 'start'), 400, (d) => d.finish - d.start);
    return (
      <div>
        <h4>Map <i className="map-label" /> / <i className="reduce-label" /> Reduce Waterfall</h4>
        {data.length ? <Waterfall data={data} /> : null}
      </div>
    );
  }
}
