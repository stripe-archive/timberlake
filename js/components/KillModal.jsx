import React from 'react';

export default class extends React.Component {
  render() {
    const {killJob, onHideModal} = this.props;
    return (
      <div className="modal show" tabIndex="-1" role="dialog">
        <div className="modal-dialog modal-kill">
          <div className="modal-content">
            <div className="modal-body">
              <h4>Are you sure you want to kill this job?</h4>
            </div>
            <div className="modal-footer">
              <button onClick={onHideModal} className="btn btn-default">Close</button>
              <button onClick={killJob} className="btn btn-danger">Kill</button>
            </div>
          </div>
        </div>
      </div>
    );
  }
}
