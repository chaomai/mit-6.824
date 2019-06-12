package master

import (
	"errors"
	"fmt"
	"gfs"
	"strings"
	"sync"

	log "github.com/Sirupsen/logrus"
)

type namespaceManager struct {
	root *nsTree
}

type nsTree struct {
	sync.RWMutex

	// if it is a directory
	isDir    bool
	children map[string]*nsTree

	// if it is a file
	length int64
	chunks int64
}

func newNamespaceManager() *namespaceManager {
	nm := &namespaceManager{
		root: &nsTree{isDir: true,
			children: make(map[string]*nsTree)},
	}
	return nm
}

func (nm *namespaceManager) dirAndLeafName(p gfs.Path) (dirParts []string, leafName string, err error) {
	path := strings.TrimLeft(string(p), "/")
	items := strings.Split(string(path), "/")
	log.Infof("dirAndFileName, split[%s]", items)
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

func (nm *namespaceManager) lockParents(dirParts []string) (parentNode *nsTree, err error) {
	log.Infof("lockParents, lock path [%s]", dirParts)

	curNode := nm.root
	for i, dir := range dirParts {
		log.Infof("lockParents, lock [%s]", dir)

		if _, ok := curNode.children[dir]; !ok {
			err = fmt.Errorf("lockParents, path %s doesn't exist", strings.Join(dirParts[0:i], "/"))
			return
		}

		if !curNode.children[dir].isDir {
			err = fmt.Errorf("lockParents, path %s isn't a directory", strings.Join(dirParts[0:i], "/"))
			return
		}

		curNode.RLock()
		curNode = curNode.children[dir]
	}

	parentNode = curNode
	return
}

func (nm *namespaceManager) unlockParents(dirParts []string) {
	parentPath := make([]*nsTree, 0)

	curNode := nm.root
	for _, dir := range dirParts {
		parentPath = append(parentPath, curNode)
		curNode = curNode.children[dir]
	}

	l := len(parentPath)
	for i := range parentPath {
		parentPath[l-i-1].RUnlock()
	}
}

// Create creates an empty file on path p. All parents should exist.
func (nm *namespaceManager) Create(p gfs.Path) error {
	dirParts, leafName, err := nm.dirAndLeafName(p)
	if err != nil {
		return err
	}

	parentNode, err := nm.lockParents(dirParts)
	if err != nil {
		return err
	}

	defer nm.unlockParents(dirParts)

	parentNode.Lock()
	defer parentNode.Unlock()

	if _, ok := parentNode.children[leafName]; ok {
		return fmt.Errorf("Create, file [%s] exists", p)
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

	parentNode, err := nm.lockParents(dirParts)
	if err != nil {
		return err
	}

	defer nm.unlockParents(dirParts)

	parentNode.Lock()
	defer parentNode.Unlock()

	if _, ok := parentNode.children[leafName]; ok {
		return fmt.Errorf("directory [%s] exists", p)
	}

	fileNode := new(nsTree)
	fileNode.isDir = true
	fileNode.children = make(map[string]*nsTree)
	parentNode.children[leafName] = fileNode

	return nil
}

func (nm *namespaceManager) List(p gfs.Path, r *gfs.ListReply) error {
	type node struct {
		name string
		nsT  *nsTree
	}

	nodes := make([]node, 0)
	nodes = append(nodes, node{"/", nm.root})

	for {
		if len(nodes) == 0 {
			break
		}

		n := nodes[0]
		nodes = nodes[1:]

		log.Infof("List, find [%s]", n.name)

		if n.nsT.isDir {
			for name, nsT := range n.nsT.children {
				nodes = append(nodes, node{name, nsT})
			}

			r.Files = append(r.Files, gfs.PathInfo{Name: n.name, IsDir: n.nsT.isDir})
		}

		if !n.nsT.isDir {
			r.Files = append(r.Files, gfs.PathInfo{Name: n.name, IsDir: n.nsT.isDir, Length: n.nsT.length, Chunks: n.nsT.chunks})
		}
	}

	return nil
}
