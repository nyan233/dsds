package btree

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"slices"
)

// Entry 表示键值对
type Entry struct {
	diskStart uint32
	Key       string
	Value     []byte
}

func (e *Entry) unmarshal(buf []byte) {
	keyLen := binary.BigEndian.Uint32(buf)
	buf = buf[4:]
	e.Key = string(buf[:keyLen])
	buf = buf[keyLen:]
	valLen := binary.BigEndian.Uint32(buf)
	buf = buf[4:]
	e.Value = buf[:valLen]
	buf = buf[valLen:]
}

func (e *Entry) marshal(buf []byte) []byte {
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(e.Key)))
	buf = append(buf, e.Key...)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(e.Value)))
	buf = append(buf, e.Value...)
	return buf
}

type diskBtreeNode struct {
	dp *diskPageMgr
	// start page desc
	startPage   *pageItem
	pdLinkCache *pageLinkCache
	entries     []Entry
	children    []*diskBtreeNode
	flags       uint64
	isLoad      bool
	m           int
}

func (d *diskBtreeNode) Reset() {
	d.isLoad = false
	d.flags = 0
	d.entries = nil
	d.children = nil
	d.pdLinkCache = nil
}

func (d *diskBtreeNode) IsLeaf() bool {
	return d.flags&1 > 0
}

func (d *diskBtreeNode) SetLeaf(b bool) {
	if b {
		d.flags |= 1
	} else {
		d.flags &= math.MaxUint64 - 1
	}
}

func (d *diskBtreeNode) invalidChildrenCache() {
	for _, child := range d.children {
		child.Reset()
	}
}

func (d *diskBtreeNode) load() error {
	d.Reset()
	var (
		buf             bytes.Buffer
		readBytes       []byte
		childrenNoBytes []byte
		err             error
	)
	if d.startPage == nil || d.startPage.pageId == 0 {
		return errors.New("invalid start page")
	}
	d.startPage, err = d.dp.ReadPage(d.startPage.pageId)
	if err != nil {
		return err
	}
	if d.startPage.pageType == 0 || d.startPage.pageId == 0 {
		return errors.New("invalid start page")
	}
	d.pdLinkCache = newPageLinkCache(d.startPage.pageId, []uint32{})
	for pd, err := range d.dp.RangeForm(d.startPage.pageId) {
		if err != nil {
			return err
		}
		buf.Write(pd.GetPayload())
		if pd.nextPageId > 0 {
			d.pdLinkCache.appendLink(pd.nextPageId)
		}
	}
	readBytes = buf.Bytes()
	flagsBytes := readBytes[:8]
	readBytes = readBytes[8:]
	d.flags = binary.BigEndian.Uint64(flagsBytes)
	childrenNoBytes = readBytes[:4*d.m]
	readBytes = readBytes[4*d.m:]
	for i := 0; i < len(childrenNoBytes); i += 4 {
		childrenNo := binary.BigEndian.Uint32(childrenNoBytes[i : i+4])
		// 0为无效值, 说明遍历到了实际的末尾
		if childrenNo == 0 {
			break
		}
		d.children = append(d.children, &diskBtreeNode{
			dp: d.dp,
			startPage: &pageItem{
				pageMgr: d.startPage.pageMgr,
				pageId:  childrenNo,
			},
			isLoad: false,
			m:      d.m,
		})
	}
	for i := 0; i < d.m*4; i += 4 {
		start := binary.BigEndian.Uint32(readBytes[i : i+4])
		if start == 0 {
			break
		}
		entryBuf := readBytes[start-uint32(8+d.m*4):]
		entry := Entry{
			diskStart: start,
		}
		entry.unmarshal(entryBuf)
		d.entries = append(d.entries, entry)
	}
	d.isLoad = true
	return nil
}

func (d *diskBtreeNode) syncConfig() error {
	return d.writeNodeData(0, binary.BigEndian.AppendUint64(nil, d.flags), nil)
}

func (d *diskBtreeNode) shrinkEntries(idxStart, idxEnd int) error {
	d.entries = d.entries[idxStart:idxEnd]
	return d.syncEntries()
}

func (d *diskBtreeNode) syncEntries() error {
	var (
		offset             = 8 + 4*d.m
		newEntriesDiskDesc = make([]byte, 0, d.m*4)
	)
	for _, entry := range d.entries {
		newEntriesDiskDesc = binary.BigEndian.AppendUint32(newEntriesDiskDesc, entry.diskStart)
	}
	// reset after bytes
	newEntriesDiskDesc = newEntriesDiskDesc[:d.m*4]
	return d.writeNodeData(offset, newEntriesDiskDesc, nil)
}

func (d *diskBtreeNode) findPd(pageN int) (*pageItem, error) {
	var (
		curSpd = d.startPage
		err    error
	)
	if pageN > 0 {
		if d.pdLinkCache == nil {
			d.pdLinkCache = newPageLinkCache(d.startPage.pageId, []uint32{})
			for ; pageN > 0; pageN-- {
				if curSpd.HasNext() {
					curSpd, err = curSpd.Next()
					if err != nil {
						return nil, err
					}
					d.pdLinkCache.appendLink(curSpd.pageId)
				} else {
					newPd, _, err := d.dp.AllocPage(pageTypeNodeData, 1)
					if err != nil {
						return nil, err
					}
					err = curSpd.Link(newPd)
					if err != nil {
						return nil, err
					}
					curSpd = newPd
					d.pdLinkCache.appendLink(curSpd.pageId)
				}
			}
		} else {
			linkIdx := pageN - 1
			lastLinkCacheIdx := d.pdLinkCache.linkLen() - 1
			if overflow := linkIdx - lastLinkCacheIdx; overflow > 0 {
				curSpd, err = d.dp.ReadPage(d.pdLinkCache.getPageIdFormIdx(uint32(lastLinkCacheIdx)))
				if err != nil {
					return nil, err
				}
				newPd, allocPage, err := d.dp.AllocPage(pageTypeNodeData, overflow)
				if err != nil {
					return nil, err
				}
				err = curSpd.Link(newPd)
				if err != nil {
					return nil, err
				}
				curSpd = newPd
				for _, v := range allocPage {
					d.pdLinkCache.appendLink(uint32(v))
				}
			} else {
				curSpd, err = d.dp.ReadPage(d.pdLinkCache.getPageIdFormIdx(uint32(linkIdx)))
				if err != nil {
					return nil, err
				}
			}
		}
	}
	return curSpd, nil
}

func (d *diskBtreeNode) writeNodeData(offset int, data []byte, usePd **pageItem) error {
	var (
		pageN      = offset / pageAvailableData
		pageOffset = offset % pageAvailableData
		curSpd     = d.startPage
		linkCache  = d.pdLinkCache
		err        error
	)
	curSpd, err = d.findPd(pageN)
	if err != nil {
		return err
	}
	if usePd != nil {
		*usePd = curSpd
	}
	if len(curSpd.GetPayload()) == 0 || curSpd.pageType == 0 {
		curSpd, err = d.dp.ReadPage(curSpd.pageId)
		if err != nil {
			return err
		}
	}
	return d.dp.writePageData(linkCache, curSpd, pageOffset, data)
}

func (d *diskBtreeNode) shrinkChildren(idxStart, idxEnd int) error {
	d.children = d.children[idxStart:idxEnd]
	return d.syncChildren()
}

func (d *diskBtreeNode) syncChildren() error {
	newChildrenBytes := make([]byte, 0, d.m*4)
	for _, child := range d.children {
		newChildrenBytes = binary.BigEndian.AppendUint32(newChildrenBytes, child.startPage.pageId)
	}
	// reset after bytes
	newChildrenBytes = newChildrenBytes[:d.m*4]
	return d.writeNodeData(8, newChildrenBytes, nil)
}

func (d *diskBtreeNode) appendEntry(es []Entry) error {
	err := d.allocAndWriteEntrySpace(es)
	if err != nil {
		return err
	}
	d.entries = append(d.entries, es...)
	return d.syncEntries()
}

func (d *diskBtreeNode) updateEntry(idx int, e Entry) error {
	es := []Entry{e}
	err := d.allocAndWriteEntrySpace(es)
	if err != nil {
		return err
	}
	d.entries[idx] = es[0]
	return d.syncEntries()
}

func (d *diskBtreeNode) insertEntry(idx int, e Entry) error {
	es := []Entry{e}
	err := d.allocAndWriteEntrySpace(es)
	if err != nil {
		return err
	}
	d.entries = append(d.entries, Entry{})
	copy(d.entries[idx+1:], d.entries[idx:])
	d.entries[idx] = es[0]
	return d.syncEntries()
}

func (d *diskBtreeNode) allocAndWriteEntrySpace(es []Entry) error {
	type entrySpaceDesc struct {
		diskStart uint32
		diskEnd   uint32
	}
	var (
		metadataLen  = uint32(8 + d.m*4 + d.m*4)
		regionDesc   = make([]entrySpaceDesc, 0, len(d.entries))
		allocRegion  uint32
		sumDiskSpace uint32
	)
	for _, entry := range es {
		sumDiskSpace += uint32(len(entry.Key) + len(entry.Value) + 8)
	}
	if len(d.entries) == 0 {
		allocRegion = metadataLen
	} else {
		// alloc target : only append
		for _, v := range d.entries {
			regionDesc = append(regionDesc, entrySpaceDesc{
				diskStart: v.diskStart,
				diskEnd:   v.diskStart + uint32(len(v.Key)+len(v.Value)+8),
			})
		}
		slices.SortFunc(regionDesc, func(a, b entrySpaceDesc) int {
			if a.diskStart > b.diskStart {
				return 1
			} else if a.diskStart < b.diskStart {
				return -1
			} else {
				return 0
			}
		})
		allocRegion = regionDesc[len(regionDesc)-1].diskEnd
	}
	var buf = make([]byte, 0, sumDiskSpace)
	for idx, entry := range es {
		es[idx].diskStart = allocRegion + uint32(len(buf))
		buf = entry.marshal(buf)
	}
	return d.writeNodeData(int(allocRegion), buf, nil)
}

func (d *diskBtreeNode) copyTo(dst *diskBtreeNode, leaveEmptyNode bool) error {
	var (
		srcSpd = d.startPage
		dstSpd = dst.startPage
		err    error
	)
	dst.isLoad = false
	err = d.dp.CopyPage(dstSpd.pageId, srcSpd.pageId)
	if err != nil {
		return err
	}
	if !leaveEmptyNode {
		err = srcSpd.Destroy()
		if err != nil {
			return err
		}
	} else {
		// last page
		if idx := (d.m*4 + d.m*4 + 8) / pageAvailableData; idx > 0 {
			pageId := d.pdLinkCache.getPageIdFormIdx(uint32(idx - 1))
			pd, err := d.dp.ReadPage(pageId)
			if err != nil {
				return err
			}
			err = pd.Unlink(true)
			if err != nil {
				return err
			}
			d.pdLinkCache.clip(0, idx)
		}
	}
	return nil
}
