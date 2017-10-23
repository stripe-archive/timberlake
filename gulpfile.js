const gulp = require('gulp');
const browserify = require('browserify');
const babelify = require('babelify');
const source = require('vinyl-source-stream');

const paths = {
  js: ['js/*/*.jsx'],
  copy: ['css/*.css', 'js/libs/*.js', 'img/*'],
};

gulp.task('copy', function() {
  return gulp.src(paths.copy, {base: './'}).pipe(gulp.dest('static'));
});

gulp.task('babel', function() {
  return browserify({entries: 'js/app.jsx', extensions: ['.jsx'], debug: true})
    .transform('babelify', {presets: ['es2015', 'react']})
    .bundle()
    .pipe(source('bundle.js'))
    .pipe(gulp.dest('static/js'));
});

gulp.task('watch', function() {
  gulp.watch(paths.copy, ['copy']);
  gulp.watch(paths.js, ['babel']);
});

gulp.task('build', ['babel', 'copy']);
gulp.task('default', ['build', 'watch']);
