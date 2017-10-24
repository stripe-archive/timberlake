import React from 'react';

import Summarizer from './Summarizer';

export default class extends React.Component {
  render() {
    const {job} = this.props;
    const inputRecords = this.props.counters.get('TaskCounter.REDUCE_INPUT_RECORDS');
    const outputRecords = this.props.counters.get('TaskCounter.REDUCE_OUTPUT_RECORDS');
    return <Summarizer progress={job.reduces} input_records={inputRecords.reduce} output_records={outputRecords.reduce} />;
  }
}
