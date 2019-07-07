package master

import (
	"encoding/gob"
	"errors"
	"os"
	"path"
	"strings"
	"sync"

	"gfs"
	log "github.com/Sirupsen/logrus"
)

type namespaceManager struct {
	root     *nsTree
	metaFile string
}

type nsTree struct {
	sync.RWMutex

	// if it is a directory
	isDir    bool
	children map[string]*nsTree

	// if it is a file
	// TODO not used
	length int64
	chunks int64
}

type iterateNode struct {
	name string
	path string
	nsT  *nsTree
}

type persistNode struct {
	Path   string
	IsDir  bool
	Length int64
	Chunks int64
}

func newNamespaceManager(serverRoot string) *namespaceManager {
	nm := &namespaceManager{
		root: &nsTree{isDir: true,
			children: make(map[string]*nsTree)},
		metaFile: path.Join(serverRoot, gfs.NamespaceManagerMetaFileName),
	}
	return nm
}

func (nm *namespaceManager) iterate(p gfs.Path, f func(*iterateNode) error) error {
	dir, name, err := nm.dirAndLeafName(p)
	if err != nil {
		log.Errorf("iterate, err[%s]", err)
		return err
	}

	curNode := nm.root
	for _, n := range dir {
		if c, ok := curNode.children[n]; ok {
			curNode = c
		} else {
			log.Errorf("iterate, err[%s]", gfs.ErrPathNotExists)
			return gfs.ErrPathNotExists
		}
	}

	if len(name) > 0 {
		if c, ok := curNode.children[name]; ok {
			curNode = c
		} else {
			log.Errorf("iterate, err[%s]", gfs.ErrPathNotExists)
			return gfs.ErrPathNotExists
		}
	}

	nodes := make([]iterateNode, 0)
	nodes = append(nodes, iterateNode{name, string(p), curNode})

	for {
		if len(nodes) == 0 {
			break
		}

		n := nodes[0]
		nodes = nodes[1:]

		log.Debugf("List, find[%s]", n.name)

		if n.nsT.isDir {
			for name, nsT := range n.nsT.children {
				nodes = append(nodes, iterateNode{name, path.Join(n.path, name), nsT})
			}
		}

		if err := f(&n); err != nil {
			return err
		}
	}

	return nil
}

func (nm *namespaceManager) serialize() error {
	persist := make([]persistNode, 0)

	err := nm.iterate("/", func(node *iterateNode) error {
		n := node.nsT
		persist = append(persist, persistNode{Path: node.path, IsDir: n.isDir, Length: n.length, Chunks: n.chunks})
		return nil
	})
	if err != nil {
		log.Errorf("serialize, err[%v]", err)
		return err
	}

	fp, err := os.OpenFile(nm.metaFile, os.O_CREATE|os.O_WRONLY, gfs.DefaultFilePerm)
	if err != nil {
		log.Errorf("serialize, err[%v]", err)
		return err
	}

	defer fp.Close()

	enc := gob.NewEncoder(fp)
	err = enc.Encode(persist)
	if err != nil {
		log.Errorf(" serialize, err[%v]", err)
		return err
	}

	return nil
}

func (nm *namespaceManager) deserialize() error {
	if _, err := os.Stat(nm.metaFile); os.IsNotExist(err) {
		log.Infof("deserialize, err[%v]", err)
		return nil
	} else if err != nil {
		log.Errorf("deserialize, err[%v]", err)
		return err
	}

	fp, err := os.OpenFile(nm.metaFile, os.O_CREATE|os.O_RDONLY, gfs.DefaultFilePerm)
	if err != nil {
		log.Errorf("deserialize, err[%v]", err)
		return err
	}

	defer fp.Close()

	persist := make([]persistNode, 0)

	dec := gob.NewDecoder(fp)
	err = dec.Decode(&persist)
	if err != nil {
		log.Errorf("deserialize, err[%v]", err)
		return err
	}

	for _, n := range persist {
		dir, name, err := nm.dirAndLeafName(gfs.Path(n.Path))
		if err != nil {
			log.Errorf("deserialize, err[%v]", err)
			return err
		}

		parent, err := nm.goThroughParents(dir, false, false, false)
		if err != nil {
			log.Errorf("deserialize, err[%v]", err)
			return err
		}

		if len(name) != 0 {
			if parent.children == nil {
				parent.children = make(map[string]*nsTree)
			}

			parent.children[name] = &nsTree{isDir: n.IsDir, length: n.Length, chunks: n.Chunks}
		}
	}

	return nil
}

func (nm *namespaceManager) dirAndLeafName(p gfs.Path) (dirParts []string, leafName string, err error) {
	tp := strings.TrimLeft(string(p), "/")
	items := strings.Split(string(tp), "/")
	log.Debugf("dirAndFileName, split[%s]", items)
	l := len(items)

	if l < 1 {
		err = errors.New("dirAndFileName, format error in path")
	} else {
		dirParts = items[0 : l-1]
		leafName = items[l-1]
	}

	log.Infof("dirAndFileName, dir[%s] file[%s]", dirParts, leafName)

	return
}

func (nm *namespaceManager) goThroughParents(dirParts []string, doLock bool, doLockParent bool, isRLockParent bool) (parentNode *nsTree, err error) {
	log.Debugf("goThroughParents, lock path[%s]", dirParts)

	curNode := nm.root
	for i, dir := range dirParts {
		log.Debugf("goThroughParents, lock node[%s]", dir)

		if _, ok := curNode.children[dir]; !ok {
			log.Errorf("goThroughParents, path[%s], err[%v]", strings.Join(dirParts[0:i], "/"), gfs.ErrPathNotExists)
			err = gfs.ErrPathNotExists
			return
		}

		if !curNode.children[dir].isDir {
			log.Errorf("goThroughParents, path[%s], err[%v]", strings.Join(dirParts[0:i], "/"), gfs.ErrPathIsNotDirectory)
			err = gfs.ErrPathIsNotDirectory
			return
		}

		if doLock {
			curNode.RLock()
		}

		curNode = curNode.children[dir]
	}

	parentNode = curNode

	if doLockParent {
		if isRLockParent {
			parentNode.RLock()
		} else {
			parentNode.Lock()
		}
	}

	return
}

func (nm *namespaceManager) unlockParents(dirParts []string, isRLockParent bool) {
	log.Debugf("unlockParents, lock path[%s]", dirParts)

	parentPath := make([]*nsTree, 0)

	curNode := nm.root
	for _, dir := range dirParts {
		parentPath = append(parentPath, curNode)
		curNode = curNode.children[dir]
	}

	if isRLockParent {
		curNode.RUnlock()
	} else {
		curNode.Unlock()
	}

	l := len(parentPath)
	for i := range parentPath {
		log.Debugf("unlockParents, unlock node[%s]", dirParts[l-i-1])
		parentPath[l-i-1].RUnlock()
	}
}

func (nm *namespaceManager) GetFileInfo(p gfs.Path) (isDir bool, length int64, chunks int64, err error) {
	dirParts, leafName, err := nm.dirAndLeafName(p)
	if err != nil {
		return
	}

	parentNode, err := nm.goThroughParents(dirParts, true, true, true)
	if err != nil {
		return
	}
	defer nm.unlockParents(dirParts, true)

	if _, ok := parentNode.children[leafName]; !ok {
		err = gfs.ErrFileNotExists
		log.Infof("Create, file[%s], err[%v]", p, err)
		return
	}

	child := parentNode.children[leafName]
	child.RLock()
	defer child.RUnlock()

	isDir = child.isDir
	length = child.length
	chunks = child.chunks

	return
}

// Create creates an empty file on path p. All parents should exist.
func (nm *namespaceManager) Create(p gfs.Path) error {
	dirParts, leafName, err := nm.dirAndLeafName(p)
	if err != nil {
		return err
	}

	parentNode, err := nm.goThroughParents(dirParts, true, true, false)
	if err != nil {
		return err
	}
	defer nm.unlockParents(dirParts, false)

	if _, ok := parentNode.children[leafName]; ok {
		log.Infof("Create, file[%s], err[%v]", p, gfs.ErrFileExists)
		return gfs.ErrFileExists
	}

	fileNode := new(nsTree)
	fileNode.isDir = false
	parentNode.children[leafName] = fileNode

	return nil
}

// Mkdir creates a directory on path p. All parents should exist.
func (nm *namespaceManager) Mkdir(p gfs.Path) error {
	dirParts, leafName, err := nm.dirAndLeafName(p)
	if err != nil {
		return err
	}

	parentNode, err := nm.goThroughParents(dirParts, false, true, false)
	if err != nil {
		return err
	}
	defer nm.unlockParents(dirParts, false)

	if _, ok := parentNode.children[leafName]; ok {
		log.Errorf("Mkdir, directory[%s], err[%v]", p, gfs.ErrDirectoryExists)
		return gfs.ErrDirectoryExists
	}

	fileNode := new(nsTree)
	fileNode.isDir = true
	fileNode.children = make(map[string]*nsTree)
	parentNode.children[leafName] = fileNode

	return nil
}

func (nm *namespaceManager) List(p gfs.Path) (r []gfs.PathInfo, err error) {
	root := make([]string, 0)
	_, err = nm.goThroughParents(root, true, true, true)
	if err != nil {
		log.Errorf("List, path[%v], err[%v]", p, err)
		return
	}
	defer nm.unlockParents(root, true)

	err = nm.iterate(p, func(node *iterateNode) error {
		r = append(r, gfs.PathInfo{Name: node.name,
			Path:   node.path,
			IsDir:  node.nsT.isDir,
			Length: node.nsT.length,
			Chunks: node.nsT.chunks,
		})

		return nil
	})
	if err != nil {
		log.Errorf("List, path[%v], err[%v]", p, err)
		return
	}

	return
}
