package btree

import (
	"fmt"
	"sort"
	"strings"
)

// BTree 表示 B 树
type BTree struct {
	nodeMgr *diskBtreeNodeMgr
	root    *diskBtreeNode
	m       int // 阶数
}

// NewBTree 创建一个新的 B 树
func NewBTree(group, name string, m int) *BTree {
	nodeMgr := newDiskBtreeNodeMgr(group, name, m)
	return &BTree{nodeMgr: nodeMgr, m: m}
}

func (t *BTree) Init() error {
	return t.nodeMgr.Init()
}

// Insert 插入一个键值对
func (t *BTree) Insert(key string, value []byte) error {
	var err error
	if t.root == nil {
		t.root, err = t.nodeMgr.loadRootNode()
		if err != nil {
			return err
		}
		if t.root == nil {
			t.root, err = t.nodeMgr.createNewNode()
			if err != nil {
				return err
			}
			t.root.SetLeaf(true)
			err = t.root.syncConfig()
			if err != nil {
				return err
			}
		}
	}
	defer func() {
		t.root.Reset()
	}()
	root := t.root
	if !root.isLoad {
		err = root.load()
		if err != nil {
			return err
		}
	}
	if len(root.entries) == t.m-1 {
		copyRoot, err := t.nodeMgr.copyAndFree(root, true)
		if err != nil {
			return err
		}
		root.SetLeaf(false)
		err = root.syncConfig()
		if err != nil {
			return err
		}
		root.children = append(root.children, copyRoot)
		err = t.splitChild(root, 0)
		if err != nil {
			return err
		}
		err = t.insertNonFull(root, key, value)
		if err != nil {
			return err
		} else {
			return nil
		}
	} else {
		return t.insertNonFull(root, key, value)
	}
}

// insertNonFull 将键值对插入非满节点
func (t *BTree) insertNonFull(node *diskBtreeNode, key string, value []byte) error {
	var err error
	if !node.isLoad {
		err = node.load()
		if err != nil {
			return err
		}
	}
	if node.IsLeaf() {
		// 使用二分搜索找到插入位置
		i := sort.Search(len(node.entries), func(i int) bool { return node.entries[i].Key >= key })
		return node.insertEntry(i, Entry{Key: key, Value: value})
	} else {
		// 使用二分搜索找到子节点位置
		i := sort.Search(len(node.entries), func(i int) bool { return node.entries[i].Key >= key })
		if !node.children[i].isLoad {
			err = node.children[i].load()
			if err != nil {
				return err
			}
		}
		if len(node.children[i].entries) == t.m-1 {
			err = t.splitChild(node, i)
			if err != nil {
				return err
			}
			if key > node.entries[i].Key {
				i++
			}
		}
		return t.insertNonFull(node.children[i], key, value)
	}
}

// splitChild 分裂满子节点
func (t *BTree) splitChild(parent *diskBtreeNode, i int) error {
	m := t.m
	child := parent.children[i]
	if !child.isLoad {
		err := child.load()
		if err != nil {
			return err
		}
	}
	mid := (m - 1) / 2 // 中间键索引

	// 保存中间键
	midEntry := child.entries[mid]

	// 创建新节点
	newNode, err := t.nodeMgr.createNewNode()
	if err != nil {
		return err
	}
	newNode.SetLeaf(child.IsLeaf())
	err = newNode.appendEntry(child.entries[mid+1:]) // 右半部分
	if err != nil {
		return err
	}
	child.entries = child.entries[:mid] // 左半部分
	if newNode.IsLeaf() {
		err = newNode.syncConfig()
		if err != nil {
			return err
		}
	}

	// 如果不是叶子，分裂子节点
	if !child.IsLeaf() {
		newNode.children = append([]*diskBtreeNode{}, child.children[mid+1:]...)
		child.children = child.children[:mid+1]
		err = child.syncChildren()
		if err != nil {
			return err
		}
		err = newNode.syncChildren()
		if err != nil {
			return err
		}
	}
	err = child.syncEntries()
	if err != nil {
		return err
	}
	// 将中间键插入父节点
	err = parent.insertEntry(i, midEntry)
	if err != nil {
		return err
	}
	// 更新父节点的子节点
	parent.children = append(parent.children, nil)
	for j := len(parent.children) - 1; j > i+1; j-- {
		parent.children[j] = parent.children[j-1]
	}
	parent.children[i+1] = newNode
	err = parent.syncChildren()
	if err != nil {
		return err
	} else {
		return nil
	}
}

// Search 查找键对应的值
func (t *BTree) Search(key string) ([]byte, bool, error) {
	node, pos, err := t.search(t.root, key)
	if err != nil {
		return nil, false, err
	}
	defer t.root.Reset()
	if node != nil {
		return node.entries[pos].Value, true, nil
	}
	return nil, false, nil
}

// search 使用二分搜索查找键
func (t *BTree) search(node *diskBtreeNode, key string) (*diskBtreeNode, int, error) {
	if !node.isLoad {
		err := node.load()
		if err != nil {
			return nil, 0, err
		}
	}
	i := sort.Search(len(node.entries), func(i int) bool { return node.entries[i].Key >= key })
	if i < len(node.entries) && node.entries[i].Key == key {
		return node, i, nil
	}
	if node.IsLeaf() {
		return nil, -1, nil
	}
	return t.search(node.children[i], key)
}

// Print 打印 B 树（用于调试）
func (t *BTree) Print() {
	t.printNode(t.root, 0)
}

func (t *BTree) printNode(node *diskBtreeNode, level int) {
	if !node.isLoad {
		t.printNode(t.root, level)
	}
	keys := make([]string, len(node.entries))
	for i, entry := range node.entries {
		keys[i] = fmt.Sprintf("%s:%v", entry.Key, entry.Value)
	}
	fmt.Printf("Level %d: [%s]\n", level, strings.Join(keys, ", "))
	for i := 0; i < len(node.children); i++ {
		t.printNode(node.children[i], level+1)
	}
}
