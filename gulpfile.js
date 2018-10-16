const gulp = require('gulp');
const browserify = require('browserify');
const source = require('vinyl-source-stream');

const paths = {
  js: ['js/**/*.jsx', 'js/**/*.js'],
  copy: ['css/*.css', 'js/libs/*.js', 'img/*'],
};

gulp.task('copy', function() {
  return gulp.src(paths.copy, {base: './'}).pipe(gulp.dest('static'));
});

gulp.task('babel', function() {
  return browserify({entries: 'js/app.jsx', extensions: ['.jsx'], debug: true})
    .transform('babelify', {presets: ['env', 'react']})
    .bundle()
    .pipe(source('bundle.js'))
    .pipe(gulp.dest('static/js'));
});

gulp.task('watch', function() {
  gulp.watch(paths.copy, ['copy']);
  gulp.watch(paths.js, ['babel']);
});

gulp.task('build', gulp.series('babel', 'copy'));
gulp.task('default', gulp.series('build', 'watch'));
