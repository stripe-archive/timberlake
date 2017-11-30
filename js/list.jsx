import React from 'react';
import {hashHistory} from 'react-router';
import {FinishedJobs, RunningJobs} from './JobTable';

const {_} = window;

export default class extends React.Component {
  constructor(props) {
    super(props);

    this.handleOnFilter = this.handleOnFilter.bind(this);

    this.state = {
      filter: '',
      flushed: true,
    };
  }

  componentDidMount() {
    document.title = 'Timberlake :: The 20/20 Experience';

    // Two reasons for debouncing:
    // 1. Transitioning on every keypress was leading to dropped keys.
    // 2. Debouncing prevents every single keypress from becoming a history item.
    this.updateLocation = _.debounce(() => {
      const q = {filter: this.state.filter};
      hashHistory.push({
        pathname: '/',
        query: _.extend(this.props.location.query, q),
      });
      this.setState({flushed: true});
    }, 250, false);
  }

  componentWillReceiveProps(nextProps) {
    // A change to the filter when we're flushed indicates a legit transition, probably via browser
    // back button. If we're not flushed then it's just the location bar catching up to our state.
    if (this.state.flushed && this.state.filter !== nextProps.location.query.filter) {
      this.setState({filter: nextProps.location.query.filter || ''});
    }
  }

  handleOnFilter(e) {
    this.updateLocation();
    this.setState({filter: e.target.value, flushed: false});
  }

  render() {
    const {jobs} = this.props;
    const filter = this.state.filter.toLowerCase();
    const isMulticluster = (new Set(jobs.map((job) => job.cluster))).size > 1;
    const props = {
      filter,
      isMulticluster,
      jobs,
      onFilter: this.handleOnFilter,
      query: this.props.location.query,
    };
    return (
      <div>
        <RunningJobs {...props} />
        <FinishedJobs {...props} />
      </div>
    );
  }
}
