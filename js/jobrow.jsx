import React from 'react';
import {Link, hashHistory} from 'react-router';

import {
  timeFormat,
  secondFormat,
  jobState,
} from './utils';
import ProgressBar from './components/progress-bar';

// Lifted from react-router.
const isLeftClickEvent = (e) => e.button === 0;
const isModifiedEvent = (event) => event.metaKey || event.altKey || event.ctrlKey || event.shiftKey;

class JobRow extends React.Component {
  constructor() {
    super();
    this.handleOnClick = this.handleOnClick.bind(this);
  }

  handleOnClick(e) {
    if (isLeftClickEvent(e) && !isModifiedEvent(e)) {
      hashHistory.push(`/job/${this.props.job.id}`);
    }
  }

  render() {
    const columns = this.columns();
    return <tr onClick={this.handleOnClick}>{columns.map((d, i) => <td key={i}>{d}</td>)}</tr>; // eslint-disable-line react/no-array-index-key
  }
}

export class RunningJobRow extends JobRow {
  columns() {
    const {job} = this.props;
    return [
      job.user,
      <Link to={`/job/${job.id}`}>{job.name}</Link>,
      timeFormat(job.startTime),
      secondFormat(job.duration()),
      <ProgressBar value={job.maps.progress} />,
      <ProgressBar value={job.reduces.progress} />,
    ];
  }
}

export class FinishedJobRow extends JobRow {
  columns() {
    const {job} = this.props;
    return [
      job.user,
      <Link to={`/job/${job.id}`}>{job.name}</Link>,
      timeFormat(job.startTime),
      timeFormat(job.finishTime),
      secondFormat(job.duration()),
      jobState(job),
    ];
  }
}
