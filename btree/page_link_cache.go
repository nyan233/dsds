package btree

type pageLinkCache struct {
	startPageId uint32
	link        *[]uint32
}

func newPageLinkCache(startPageId uint32, link []uint32) *pageLinkCache {
	return &pageLinkCache{
		startPageId: startPageId,
		link:        &link,
	}
}

func (obj *pageLinkCache) transDiskOff(diskOff int) (uint32, uint32, uint32) {
	var (
		pageIdx      = diskOff / pageAvailableData
		pageInnerOff = diskOff % pageAvailableData
	)
	if pageIdx >= obj.linkLen() {
		return uint32(len(*obj.link) - 1), 0, uint32(pageIdx - len(*obj.link) + 1)
	} else {
		return uint32(pageIdx), uint32(pageInnerOff), 0
	}
}

func (obj *pageLinkCache) linkLen() int {
	if obj.link == nil {
		return 0
	} else {
		return len(*obj.link)
	}
}

func (obj *pageLinkCache) getPageIdFormIdx(idx uint32) uint32 {
	return (*obj.link)[idx]
}

func (obj *pageLinkCache) appendLink(next uint32) {
	*obj.link = append(*obj.link, next)
}

func (obj *pageLinkCache) next() *pageLinkCache {
	return obj.subWithIndex(0)
}

func (obj *pageLinkCache) subWithIndex(idx uint32) *pageLinkCache {
	newLink := (*obj.link)[idx+1:]
	return &pageLinkCache{
		startPageId: (*obj.link)[idx],
		link:        &newLink,
	}
}

func (obj *pageLinkCache) clip(i, j int) {
	*obj.link = (*obj.link)[i:j]
}
