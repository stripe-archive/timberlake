var gulp = require('gulp');
var react = require('gulp-react');

var paths = {
  js: ['js/*.js'],
  jsx: ['js/*.jsx'],
  copy: ['js/*.js', 'css/*.css', 'js/libs/*.js', 'img/*'],
};

gulp.task('copy', function() {
  return gulp.src(paths.copy, {base: './'}).pipe(gulp.dest('static'));
});

gulp.task('react', function() {
  return gulp.src(paths.jsx)
    .pipe(react({harmony: true}))
    .pipe(gulp.dest('static/js'));
});

gulp.task('watch', function() {
  gulp.watch(paths.copy, ['copy']);
  gulp.watch(paths.jsx, ['react']);
});

gulp.task('build', ['react', 'copy']);
gulp.task('default', ['build', 'watch']);
