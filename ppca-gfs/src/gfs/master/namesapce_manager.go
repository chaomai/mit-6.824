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

func (nm *namespaceManager) dirAndFileName(p gfs.Path) (dirParts []string, fileName string, err error) {
	path := strings.TrimLeft(string(p), "/")
	items := strings.Split(string(path), "/")
	log.Infof("dirAndFileName split[%s]", items)
	l := len(items)

	if l < 1 {
		err = errors.New("format error in path")
	} else {
		dirParts = items[0 : l-1]
		fileName = items[l-1]
	}

	log.Infof("dirAndFileName dir[%s] file[%s]", dirParts, fileName)

	return
}

func (nm *namespaceManager) lockParents(dirParts []string) (parentNode *nsTree, err error) {
	log.Infof("lock path [%s]", dirParts)

	curNode := nm.root
	curNode.RLock()
	for i, dir := range dirParts {
		log.Infof("lock [%s]", dir)

		if _, ok := curNode.children[dir]; !ok {
			err = fmt.Errorf("path %s doesn't exist", strings.Join(dirParts[0:i], "/"))
			return
		}

		curNode = curNode.children[dir]
		curNode.RLock()
	}

	parentNode = curNode
	return
}

func (nm *namespaceManager) unlockParents(dirParts []string) {
	parentPath := make([]*nsTree, 0)
	curNode := nm.root
	parentPath = append(parentPath, curNode)

	for _, dir := range dirParts {
		curNode = curNode.children[dir]
		parentPath = append(parentPath, curNode)
	}

	l := len(parentPath)
	for i := range parentPath {
		parentPath[l-i-1].RUnlock()
	}
}

// Create creates an empty file on path p. All parents should exist.
func (nm *namespaceManager) Create(p gfs.Path) error {
	dirParts, fileName, err := nm.dirAndFileName(p)
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

	fileNode := new(nsTree)
	fileNode.isDir = false
	parentNode.children[fileName] = fileNode

	return nil
}

// Mkdir creates a directory on path p. All parents should exist.
func (nm *namespaceManager) Mkdir(p gfs.Path) error {
	return nil
}
