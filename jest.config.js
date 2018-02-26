require('babel-jest');

module.exports = {
  roots: ['js'],
  moduleFileExtensions: ['js', 'jsx'],
  transform: {
    '\\.jsx?$': 'babel-jest',
  },
};
