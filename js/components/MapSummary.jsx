import React from 'react';

import Summarizer from './Summarizer';

export default class extends React.Component {
  render() {
    const {job} = this.props;
    const inputRecords = this.props.counters.get('TaskCounter.MAP_INPUT_RECORDS');
    const outputRecords = this.props.counters.get('TaskCounter.MAP_OUTPUT_RECORDS');
    return <Summarizer progress={job.maps} input_records={inputRecords.map} output_records={outputRecords.map} />;
  }
}
