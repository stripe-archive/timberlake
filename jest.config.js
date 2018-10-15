require('babel-jest');

module.exports = {
  testURL: 'http://localhost/',
  roots: ['js'],
  moduleFileExtensions: ['js', 'jsx'],
  transform: {
    '\\.jsx?$': 'babel-jest',
  },
};
