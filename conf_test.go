package main

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadConf(t *testing.T) {
	f, err := os.Open("test/conf.xml")
	require.NoError(t, err, "test/conf.xml should load")

	job := job{}
	conf, err := loadConf(f)
	job.conf.update(conf)

	require.NoError(t, err, "should be able to load conf correctly")
	assert.Equal(t, "/input/dir", job.conf.Input, "input should have loaded correctly")
	assert.Equal(t, "/output/dir", job.conf.Output, "output should have loaded correctly")
	assert.Equal(t, "appname", job.conf.name, "name should have loaded correctly")
}
