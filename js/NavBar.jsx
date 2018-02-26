import React from 'react';
import {numFormat} from './utils/d3';

/**
 * Component responsible for rendering the top navigation bar.
 */
export class NavBar extends React.Component {
  render() {
    const running = this.props.jobs.filter((j) => j.state === 'RUNNING');
    const mappers = running.map((j) => j.maps.running).reduce((x, y) => x + y, 0);
    const reducers = running.map((j) => j.reduces.running).reduce((x, y) => x + y, 0);

    return (
      <nav className="navbar navbar-default">
        <div className="container">
          <div className="navbar-header">
            <a className="navbar-brand" href="#">Timberlake</a>
          </div>
          <div className="navbar-right">
            <p className="navbar-text">mappers: {numFormat(mappers)}</p>
            <p className="navbar-text">reducers: {numFormat(reducers)}</p>
          </div>
        </div>
      </nav>
    );
  }
}
