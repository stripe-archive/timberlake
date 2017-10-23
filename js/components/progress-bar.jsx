import React from 'react';

export default class extends React.Component {
  render() {
    const n = Math.max(0, Math.min(100, isNaN(this.props.value) ? 0 : this.props.value));
    const val = `${n.toFixed(2)}%`;
    return (
      <div className="pct">
        <div className="bar positive" style={{width: `${n}%`}}><span>{val}</span></div>
        <div className="bar negative" style={{width: `${100 - n}%`}}><span>{val}</span></div>
      </div>
    );
  }
}

