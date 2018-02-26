// @flow
import {jobLabel} from '../utils';

test('label', () => {
  expect(jobLabel('job.FilterSnapshotJob$ (execution-step 0)/(1/2) ...180223/filtered_snapshots')).toBe('1');
});
