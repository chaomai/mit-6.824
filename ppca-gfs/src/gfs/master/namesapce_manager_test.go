package master

import (
	"io/ioutil"
	"testing"

	"gfs"
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

	dir, file, err = nm.dirAndLeafName("/")
	t.Log(dir)
	t.Log(file)
	t.Log(err)
}

func Test_namespaceManager_serialize(t *testing.T) {
	root, _ := ioutil.TempDir("", "gfs-")
	t.Log(root)
	nm := newNamespaceManager(root)

	nm.Mkdir(gfs.Path("/dir1"))
	nm.Mkdir(gfs.Path("/dir2"))
	nm.Create(gfs.Path("/file1.txt"))
	nm.Create(gfs.Path("/file2.txt"))
	nm.Create(gfs.Path("/dir1/file3.txt"))
	nm.Create(gfs.Path("/dir1/file4.txt"))
	nm.Create(gfs.Path("/dir2/file5.txt"))

	nm.serialize()
	nm.root = &nsTree{isDir: true, children: make(map[string]*nsTree)}
	nm.deserialize()

	t.Log(nm.List("/"))
}
