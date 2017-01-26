import React from 'react';

export var paddedInt = d3.format("02d");
export var timeFormat = d3.time.format.utc("%a %H:%M:%S");
export var lolhadoop = s => s.replace(/application|job/, '');

export function numFormat(n) {
  if (!n) n = 0;
  return d3.format(",d")(n);
}

export function secondFormat(n) {
  n = n / 1000;
  var hour = Math.floor(n / 3600);
  var minute = Math.floor(n % 3600 / 60);
  var second = Math.floor(n % 3600 % 60);
  return numFormat(hour) + ":" + paddedInt(minute) + ":" + paddedInt(second);
}

export function plural(n, s) {
  return n == 1 ? s : s + "s";
}

export function humanFormat(n) {
  n = n / 1000;
  var hour = Math.floor(n / 3600);
  var minute = Math.floor(n % 3600 / 60);
  var second = Math.floor(n % 3600 % 60);
  if (n < 60) {
    return second + plural(second, " second");
  } else if (n < 3600) {
    return minute + plural(minute, " minute");
  }
  return numFormat(hour) + plural(hour, " hour") + " " + minute + plural(minute, " minute");
}


export var ACTIVE_STATES = ['RUNNING', 'ACCEPTED'];
export var FINISHED_STATES = ['SUCCEEDED', 'KILLED', 'FAILED', 'ERROR'];
export var FAILED_STATES = ['FAILED', 'KILLED', 'ERROR'];

export function jobState(job) {
  var state = job.state;
  var label = {
    'accepted': 'success',
    'succeeded': 'success',
    'killed': 'warning',
    'failed': 'danger',
    'error': 'danger',
    'running': 'primary'
  };
  return <span className={'label label-' + label[state.toLowerCase()]}>{state}</span>;
}

export function cleanJobName(name) {
  return name.replace(/\[[A-Z0-9\/]+\]\s+/, '').replace(/(\w+\.)+(\w+)/, '$1$2');
}

export function cleanJobPath(path) {
  if (!path) return path;
  path = path.replace(/hdfs:\/\/\w+(\.\w+)*:\d+/g, '');
  path = path.replace(/,/, ', ');
  return path;
}

export const COLOUR_MAP = "rgb(91, 192, 222)";
export const COLOUR_REDUCE = "#E86482";
export const COLOUR_SELECTED = "rgb(100, 232, 130)";
export const COLOUR_HOVER = "rgb(100, 232, 200)";
