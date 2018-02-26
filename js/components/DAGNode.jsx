import React from 'react';
import {
  COLOUR_MAP,
  COLOUR_SELECTED,
  COLOUR_HOVER,
} from '../utils/utils';

/**
 * Renders a node in the DAG view.
 */
export default class extends React.Component {
  constructor(props) {
    super(props);

    this.handleOnMouseEnter = this.handleOnMouseEnter.bind(this);
    this.handleOnMouseLeave = this.handleOnMouseLeave.bind(this);

    this.state = {
      hover: false,
    };
  }

  handleOnMouseEnter() {
    this.setState({hover: true});
    if (this.props.hover) {
      this.props.hover(this.props.node.job);
    }
  }

  handleOnMouseLeave() {
    this.setState({hover: false});
    if (this.props.hover) {
      this.props.hover(null);
    }
  }

  render() {
    const {node, selected} = this.props;

    let colour;
    if (selected) {
      colour = COLOUR_SELECTED;
    } else if (this.state.hover) {
      colour = COLOUR_HOVER;
    } else {
      colour = COLOUR_MAP;
    }

    return (
      <g
        onMouseEnter={this.handleOnMouseEnter}
        onMouseLeave={this.handleOnMouseLeave}
        transform={`translate(${node.x}, ${node.y})`}
      >
        <a href={`/#/job/${node.job.id}`}>
          <circle r="20" style={{fill: colour}} />
          <text
            style={{
              textAnchor: 'middle',
              alignmentBaseline: 'middle',
            }}
          >
            {node.label}
          </text>
        </a>
      </g>
    );
  }
}
