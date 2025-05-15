package btree

import (
	"encoding/binary"
	"errors"
)

type pageItem struct {
	pageMgr    *diskPageMgr
	rawBuf     []byte
	pageType   byte
	pageId     uint32
	nextPageId uint32
}

func parsePageItem(mgr *diskPageMgr, buf []byte) *pageItem {
	if len(buf) != defaultPageSize {
		panic("invalid buf size")
	}
	p := new(pageItem)
	p.pageMgr = mgr
	p.pageType = buf[0]
	p.pageId = binary.BigEndian.Uint32(buf[1:])
	p.nextPageId = binary.BigEndian.Uint32(buf[5:])
	p.rawBuf = buf
	return p
}

func (p *pageItem) GetPayload() []byte {
	return p.rawBuf[9:]
}

func (p *pageItem) HasNext() bool {
	return p.nextPageId != 0
}

func (p *pageItem) Flush(marshal bool) error {
	if marshal {
		p.rawBuf[0] = p.pageType
		binary.BigEndian.PutUint32(p.rawBuf[1:], p.pageId)
		binary.BigEndian.PutUint32(p.rawBuf[5:], p.nextPageId)
	}
	return p.pageMgr.WritePage(p.pageId, 0, p.rawBuf)
}

func (p *pageItem) Next() (*pageItem, error) {
	if p.nextPageId == 0 {
		return nil, errors.New("no next page")
	}
	return p.pageMgr.ReadPage(p.nextPageId)
}

func (p *pageItem) Link(dst *pageItem) error {
	if dst.pageId == 0 {
		return errors.New("invalid page id")
	}
	p.nextPageId = dst.pageId
	return p.Flush(true)
}

func (p *pageItem) Unlink(clearData bool) error {
	if p.nextPageId == 0 {
		return errors.New("invalid next page id")
	}
	if clearData {
		nextPd, err := p.Next()
		if err != nil {
			return err
		}
		err = p.pageMgr.FreePageAndLink(nextPd)
		if err != nil {
			return err
		}
	}
	p.nextPageId = 0
	return p.Flush(true)
}

func (p *pageItem) Destroy() error {
	var (
		err        error
		pageId     = p.pageId
		nextPageId = p.nextPageId
	)
	if nextPageId != 0 {
		err = p.Unlink(true)
		if err != nil {
			return err
		}
	}
	return p.pageMgr.freePages([]uint32{pageId})
}
