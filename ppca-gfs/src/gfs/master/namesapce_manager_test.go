package master

import (
	"testing"
)

func TestDirAndFileName(t *testing.T) {
	var nm *namespaceManager

	dir, file, err := nm.dirAndLeafName("/Users/chaomai/Documents/workspace/github/mit-6.824/a.txt")
	t.Log(dir)
	t.Log(file)
	t.Log(err)

	dir, file, err = nm.dirAndLeafName("/a.txt")
	t.Log(dir)
	t.Log(file)
	t.Log(err)
}
