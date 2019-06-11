package master

import (
	"testing"
)

func TestDirAndFileName(t *testing.T) {
	nm := &namespaceManager{}

	dir, file, err := nm.dirAndFileName("/Users/chaomai/Documents/workspace/github/mit-6.824/a.txt")
	t.Log(dir)
	t.Log(file)
	t.Log(err)

	dir, file, err = nm.dirAndFileName("/a.txt")
	t.Log(dir)
	t.Log(file)
	t.Log(err)
}
