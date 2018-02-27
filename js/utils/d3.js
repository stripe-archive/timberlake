// @flow
import {plural} from './utils';

const {d3} = window;

const paddedInt = d3.format('02d');
export const timeFormat = d3.time.format.utc('%a %H:%M:%S');

export function numFormat(n: number) {
  return d3.format(',d')(n || 0);
}

export function secondFormat(ms: number) {
  const n = ms / 1000;
  const hour = Math.floor(n / 3600);
  const minute = Math.floor(n % 3600 / 60);
  const second = Math.floor(n % 3600 % 60);
  return `${numFormat(hour)}:${paddedInt(minute)}:${paddedInt(second)}`;
}

export function humanFormat(ms: number) {
  const n = ms / 1000;
  const hour = Math.floor(n / 3600);
  const minute = Math.floor(n % 3600 / 60);
  const second = Math.floor(n % 3600 % 60);
  if (n < 60) {
    return second + plural(second, ' second');
  } else if (n < 3600) {
    return minute + plural(minute, ' minute');
  }
  return `${numFormat(hour) + plural(hour, ' hour')} ${minute}${plural(minute, ' minute')}`;
}
