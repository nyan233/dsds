package btree

import (
	"cmp"
	"encoding/binary"
	"fmt"
	"iter"
	"os"
	"path"
	"slices"
	"sort"
	"sync"
)

const (
	defaultPageSize        = 32 * 1024
	defaultFileNPage       = 256
	pageTypeNodeData  byte = 1
	pageTypeFreelist  byte = 2
	pageAvailableData      = defaultPageSize - 9
)

var (
	zeroBuffer = make([]byte, defaultPageSize)
)

type diskPageMgr struct {
	group         string
	name          string
	dbFile        *os.File
	bufferPool    sync.Pool
	freelistCache []uint32
	allocLogger   *diskAllocLogger
}

func newDiskPageMgr(group, name string) *diskPageMgr {
	return &diskPageMgr{
		group: group,
		name:  name,
		bufferPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 0, defaultPageSize)
			},
		},
	}
}

func (dp *diskPageMgr) Init() error {
	err := dp.openFile()
	if err != nil {
		return err
	}
	file, err := os.OpenFile(path.Join(dp.group, dp.name+".alog"), os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	dp.allocLogger = newDiskAllocLogger(file, dp.allocLogCommitHandler)
	return dp.allocLogger.init()
}

func (dp *diskPageMgr) getFileName() string {
	return path.Join(dp.group, dp.name+".db")
}

func (dp *diskPageMgr) openFile() error {
	var err error
	dp.dbFile, err = createFile(dp.getFileName())
	if err != nil {
		return err
	}
	if stat, err := dp.dbFile.Stat(); err != nil {
		return err
	} else if stat.Size() == 0 {
		err = fileAllocate(dp.dbFile, 0, defaultPageSize*defaultFileNPage)
		if err != nil {
			return fmt.Errorf("fileAllocate err : %v", err)
		}
		p := parsePageItem(dp, make([]byte, defaultPageSize))
		p.pageType = pageTypeFreelist
		// write freelist count
		binary.BigEndian.PutUint32(p.GetPayload()[:4], defaultFileNPage)
		for i := 1; i < defaultFileNPage; i++ {
			binary.BigEndian.PutUint32(p.GetPayload()[i*4:(i+1)*4], uint32(i))
		}
		err = p.Flush(true)
		if err != nil {
			return fmt.Errorf("init write data err : %v", err)
		}
	}
	var freelistCount uint32
	for page2, err2 := range dp.RangeForm(0) {
		if err2 != nil {
			return err2
		}
		payload := page2.GetPayload()
		if page2.pageId == 0 && len(dp.freelistCache) <= 0 {
			freelistCount = binary.BigEndian.Uint32(payload[:4])
			payload = payload[4:]
			dp.freelistCache = make([]uint32, 0, freelistCount)
		}
		for i := 0; i < len(payload); i += 4 {
			freePageId := binary.BigEndian.Uint32(payload[i : i+4])
			if freePageId == 0 || len(dp.freelistCache) == int(freelistCount) {
				break
			}
			dp.freelistCache = append(dp.freelistCache, freePageId)
		}
	}
	return err
}

func (dp *diskPageMgr) syncFreelist() error {
	var buf = make([]byte, 0, len(dp.freelistCache)*4+4)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(dp.freelistCache)))
	for _, pageId := range dp.freelistCache {
		buf = binary.BigEndian.AppendUint32(buf, pageId)
	}
	return dp.writePageData(nil, &pageItem{pageId: 0}, 0, buf)
}

func (dp *diskPageMgr) allocLogCommitHandler(logs []diskAllocLog) error {
	if len(logs) == 0 {
		// cache is coherence
		return dp.syncFreelist()
	} else {
		// commit of init
		for _, log := range logs {
			switch log.op {
			case freelistAllocNewPageOp:
				idx, found := sort.Find(len(dp.freelistCache), func(i int) int {
					return cmp.Compare(dp.freelistCache[i], log.pageId)
				})
				if !found {
					return fmt.Errorf("db data corrupted")
				}
				dp.freelistCache = slices.Delete(dp.freelistCache, idx, idx)
			case freelistFreePage:
				idx := sort.Search(len(dp.freelistCache), func(i int) bool {
					return dp.freelistCache[i] >= log.pageId
				})
				if idx < len(dp.freelistCache) && dp.freelistCache[idx] == log.pageId {
					// dup free page
					return fmt.Errorf("dup free page")
				} else {
					// in freelist cache interval
					dp.freelistCache = slices.Insert(dp.freelistCache, idx, log.pageId)
				}
			default:
				return fmt.Errorf("invalid op")
			}
		}
		return dp.syncFreelist()
	}
}

func (dp *diskPageMgr) CopyPage(dstPageId, srcPageId uint32) error {
	// 这里copy逻辑不要使用zero-copy, zero-copy对小数据量没有什么优势, 除非以后有大页
	dst, err := dp.ReadPage(dstPageId)
	if err != nil {
		return err
	}
	src, err := dp.ReadPage(srcPageId)
	if err != nil {
		return err
	}
	for {
		copy(dst.GetPayload(), src.GetPayload())
		err := dst.Flush(false)
		if err != nil {
			return err
		}
		if !dst.HasNext() && src.HasNext() {
			pd, _, err := dp.AllocPage(pageTypeNodeData, 1)
			if err != nil {
				return err
			}
			err = dst.Link(pd)
			if err != nil {
				return err
			}
			dst = pd
			src, err = src.Next()
			if err != nil {
				return err
			}
		} else if src.HasNext() && dst.HasNext() {
			dst, err = dst.Next()
			if err != nil {
				return err
			}
			src, err = src.Next()
			if err != nil {
				return err
			}
		} else {
			break
		}
	}
	return nil
}

// NOTE: 2GB之前的文件扩容就*2, 2GB之后每次增加1GB
func (dp *diskPageMgr) growFile() error {
	//bitmapSize := len(dp.allocBitmap2) * 8
	//dataSize := len(dp.allocBitmap2) * defaultPageSize * 64
	//err := fileAllocate(dp.dbFile, 0, int64(dataSize*2))
	//if err != nil {
	//	return err
	//}
	//pd, err := dp.ReadPage(0)
	//if err != nil {
	//	return err
	//}
	//binary.BigEndian.AppendUint32(pd.GetPayload(), uint32(bitmapSize*2))
	//err = pd.Flush(true)
	//if err != nil {
	//	return fmt.Errorf("WriteAllocMap err : %v", err)
	//}
	//oldBitmapCache := dp.allocBitmap2
	//dp.allocBitmap2 = make([]uint64, len(oldBitmapCache)*2)
	//copy(dp.allocBitmap2, oldBitmapCache)
	//return dp.syncBitmap(len(oldBitmapCache))
	stat, err := dp.dbFile.Stat()
	if err != nil {
		return err
	}
	var (
		oldSize      = stat.Size()
		oldPageCount = oldSize / defaultPageSize
		newSize      = oldSize * 2
		newPageCount = oldPageCount * 2
	)
	if newSize > 1<<31 {
		newSize = oldSize + (1 << 30)
		newPageCount = oldPageCount + (1<<30)/defaultPageSize
	}
	err = fileAllocate(dp.dbFile, 0, newSize)
	if err != nil {
		return err
	}
	err = dp.allocLogger.commit()
	if err != nil {
		return err
	}
	for i := oldPageCount; i < newPageCount; i++ {
		dp.freelistCache = append(dp.freelistCache, uint32(i))
	}
	return dp.syncFreelist()
}

//func (dp *diskPageMgr) allocPage(n int) ([]int, error) {
//	var allocPage = make([]int, 0, n)
//	for i, v := range dp.allocBitmap2 {
//		// found non-alloc page
//		for v != math.MaxUint64 {
//			// NOTE: 取反后计算低位尾随零位的数量, 由于取反了, 之前的1就是0, 所以就能算出取反前低位最近的一个为0的bit
//			bitpos := bits.TrailingZeros64(^v)
//			v |= uint64(1) << bitpos
//			dp.allocBitmap2[i] = v
//			allocPage = append(allocPage, i*64+bitpos)
//			if len(allocPage) == n {
//				goto exitLoop
//			}
//		}
//	}
//exitLoop:
//	if interval := n - len(allocPage); interval > 0 {
//		err := dp.growFile()
//		if err != nil {
//			return allocPage, err
//		}
//		page2, err := dp.allocPage(interval)
//		if err != nil {
//			return allocPage, err
//		}
//		allocPage = append(allocPage, page2...)
//		return allocPage, nil
//	}
//	return allocPage, dp.syncBitmap(allocPage[0])
//}

func (dp *diskPageMgr) allocPage(n int) ([]uint32, error) {
	// fft algo
	if n > len(dp.freelistCache) {
		err := dp.growFile()
		if err != nil {
			return nil, err
		}
	}
	allocPages := make([]uint32, n)
	copy(allocPages, dp.freelistCache[:n])
	dp.freelistCache = dp.freelistCache[n:]
	logs := make([]diskAllocLog, 0, n)
	for _, pageId := range allocPages {
		logs = append(logs, diskAllocLog{op: freelistAllocNewPageOp, pageId: pageId})
	}
	return allocPages, dp.allocLogger.writeLogs(logs)
}

// AllocPage @args : n > 1则分配连接的page
func (dp *diskPageMgr) AllocPage(pageType byte, n int) (*pageItem, []uint32, error) {
	allocPage, err := dp.allocPage(n)
	if err != nil {
		return nil, nil, err
	}
	var (
		retPageItem *pageItem
		zeroBuf     = make([]byte, defaultPageSize)
	)
	for i := 0; i < len(allocPage); i++ {
		var (
			pageId     = allocPage[i]
			nextPageId uint32
		)
		if i != len(allocPage)-1 {
			nextPageId = allocPage[i+1]
		}
		p := parsePageItem(dp, zeroBuf)
		p.pageMgr = dp
		p.pageType = pageType
		p.pageId = pageId
		p.nextPageId = nextPageId
		if retPageItem == nil {
			retPageItem = p
		}
		err = p.Flush(true)
		if err != nil {
			return nil, nil, err
		}
	}
	return retPageItem, allocPage, nil
}

func (dp *diskPageMgr) freePages(pages []uint32) error {
	var logs = make([]diskAllocLog, 0, len(pages))
	for _, page := range pages {
		if !dp.isAlloc(page) {
			return fmt.Errorf("page no alloc %d", page)
		}
		logs = append(logs, diskAllocLog{op: freelistFreePage, pageId: page})
		idx := sort.Search(len(dp.freelistCache), func(i int) bool {
			return dp.freelistCache[i] >= page
		})
		slices.Insert(dp.freelistCache, idx, page)
	}
	return dp.allocLogger.writeLogs(logs)
}

// FreePageAndLink 释放掉page和相关link page
func (dp *diskPageMgr) FreePageAndLink(pd *pageItem) (err error) {
	freePages := []uint32{
		pd.pageId,
	}
	for pd.HasNext() {
		pd, err = pd.Next()
		if err != nil {
			return err
		}
		freePages = append(freePages, pd.pageId)
	}
	return dp.freePages(freePages)
}

func (dp *diskPageMgr) StorePageData(pageId uint32, off uint16, data []byte) error {
	if int(off)+len(data) > pageAvailableData {
		return fmt.Errorf("page data size overflow")
	}
	err := dp.WritePage(pageId, int64(9+off), data)
	if err != nil {
		return err
	} else {
		return nil
	}
}

func (dp *diskPageMgr) RangeForm(startPage uint32) iter.Seq2[*pageItem, error] {
	pd, err := dp.ReadPage(startPage)
	return func(yield func(*pageItem, error) bool) {
		if !yield(pd, err) {
			return
		}
		for pd.HasNext() {
			pd, err = pd.Next()
			if !yield(pd, err) {
				return
			}
		}
	}
}

func (dp *diskPageMgr) ReadPage(pageId uint32) (*pageItem, error) {
	buf := make([]byte, defaultPageSize)
	_, err := dp.dbFile.ReadAt(buf, int64(pageId*defaultPageSize))
	if err != nil {
		return nil, err
	}
	return parsePageItem(dp, buf), nil
}

func (dp *diskPageMgr) isAlloc(n uint32) bool {
	_, found := sort.Find(len(dp.freelistCache), func(i int) int {
		return cmp.Compare(dp.freelistCache[i], n)
	})
	return !found
}

func (dp *diskPageMgr) fullWrite(buf []byte, off int64) error {
	writeCount, err := dp.dbFile.WriteAt(buf, off)
	if err != nil {
		return err
	}
	if writeCount != len(buf) {
		return fmt.Errorf("write count not equal : %d!=%d", writeCount, len(buf))
	} else {
		return nil
	}
}

func (dp *diskPageMgr) WritePage(pageId uint32, off int64, buf []byte) error {
	if len(buf) > defaultPageSize {
		return fmt.Errorf("buf size too big : %d", len(buf))
	}
	if pageId != 0 && !dp.isAlloc(pageId) {
		return fmt.Errorf("page no alloc %d", pageId)
	}
	return dp.fullWrite(buf, int64(pageId*defaultPageSize)+off)
}

func (dp *diskPageMgr) writePageData(linkCache *pageLinkCache, pd *pageItem, off int, data []byte) error {
	var err error
	if pd.pageType == 0 {
		var pd2 *pageItem
		pd2, err = dp.ReadPage(pd.pageId)
		if err != nil {
			return err
		}
		*pd = *pd2
	}
	for len(data) > 0 {
		var rewriteN int
		if len(data) < pageAvailableData-off {
			rewriteN = len(data)
		} else {
			rewriteN = pageAvailableData - off
		}
		copy(pd.GetPayload()[off:], data[:rewriteN])
		err = pd.Flush(false)
		if err != nil {
			return err
		}
		if off > 0 {
			off = 0
		}
		data = data[rewriteN:]
		if len(data) > 0 {
			if pd.HasNext() {
				pd, err = pd.Next()
				if err != nil {
					return err
				}
			} else {
				var (
					newSpd    *pageItem
					allocPage []uint32
				)
				newSpd, allocPage, err = dp.AllocPage(pageTypeNodeData, (len(data)/pageAvailableData)+1)
				if err != nil {
					return err
				}
				err = pd.Link(newSpd)
				if err != nil {
					return err
				}
				if linkCache != nil {
					for _, v := range allocPage {
						linkCache.appendLink(v)
					}
				}
				pd = newSpd
			}
		}
	}
	return nil
}
