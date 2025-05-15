package btree

const (
	createNodeMinPage = 1
)

type diskBtreeNodeMgr struct {
	dp *diskPageMgr
	m  int
}

func newDiskBtreeNodeMgr(group, name string, m int) *diskBtreeNodeMgr {
	dp := newDiskPageMgr(group, name)
	return &diskBtreeNodeMgr{dp: dp, m: m}
}

func (obj *diskBtreeNodeMgr) Init() error {
	return obj.dp.Init()
}

func (obj *diskBtreeNodeMgr) loadRootNode() (*diskBtreeNode, error) {
	root := &diskBtreeNode{
		dp: obj.dp,
		m:  obj.m,
		startPage: &pageItem{
			pageType: pageTypeNodeData,
			pageId:   1,
		},
	}
	err := root.load()
	if err != nil {
		if err.Error() == "invalid start page" {
			return nil, nil
		}
		return nil, err
	} else {
		return root, nil
	}
}

func (obj *diskBtreeNodeMgr) createNewNode() (*diskBtreeNode, error) {
	pd, allocPage, err := obj.dp.AllocPage(pageTypeNodeData, createNodeMinPage)
	if err != nil {
		return nil, err
	}
	linkCache := make([]uint32, 0, len(allocPage)-1)
	for _, page := range allocPage[1:] {
		linkCache = append(linkCache, page)
	}
	newNode := &diskBtreeNode{
		dp:          obj.dp,
		startPage:   pd,
		m:           obj.m,
		pdLinkCache: newPageLinkCache(pd.pageId, linkCache),
	}
	return newNode, nil
}

// leaveEmptyNode: 这个参数为true时则将原始节点重置为空节点, 否则释放掉节点所有相关的数据
func (obj *diskBtreeNodeMgr) copyAndFree(node *diskBtreeNode, leaveEmptyNode bool) (*diskBtreeNode, error) {
	var (
		err     error
		newNode *diskBtreeNode
	)
	newNode, err = obj.createNewNode()
	if err != nil {
		return nil, err
	}
	err = node.copyTo(newNode, leaveEmptyNode)
	if err != nil {
		return nil, err
	}
	oldLinkCache := node.pdLinkCache
	node.Reset()
	node.pdLinkCache = oldLinkCache
	node.pdLinkCache.clip(0, createNodeMinPage-1)
	return newNode, newNode.load()
}
