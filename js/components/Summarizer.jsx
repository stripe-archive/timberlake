import React from 'react';
import ProgressBar from './progress-bar';

import {
  humanFormat,
  numFormat,
  plural,
} from '../utils';
import {notAvailable} from '../mr';

export default class extends React.Component {
  render() {
    const {progress} = this.props;
    const inputRecords = this.props.input_records;
    const outputRecords = this.props.output_records;
    const recordsPerSec = inputRecords != notAvailable ? Math.floor(inputRecords / (progress.totalTime / 1000)) : null;
    const computeTime = recordsPerSec ?
      <span>{humanFormat(progress.totalTime)}<br />{numFormat(recordsPerSec)} {plural(recordsPerSec, 'record')}/sec</span>
      : <span>{humanFormat(progress.totalTime)}</span>;
    const pairs = [
      ['Progress', <ProgressBar value={progress.progress} />],
      ['Total', numFormat(progress.total)],
      ['Completed', numFormat(progress.completed)],
      ['Running', numFormat(progress.running)],
      ['Pending', numFormat(progress.pending)],
      ['Killed', numFormat(progress.killed)],
      ['Failed', numFormat(progress.failed)],
      ['Input Records', inputRecords != notAvailable ? numFormat(inputRecords) : null],
      ['Output Records', outputRecords != notAvailable ? numFormat(outputRecords) : null],
      ['Compute Time', progress.totalTime > 0 ? computeTime : null],
    ];
    return (
      <table className="table">
        <tbody>{pairs.map((t, i) => <tr key={i}><th>{t[0]}</th><td>{t[1]}</td></tr>)}</tbody>
      </table>
    );
  }
}
