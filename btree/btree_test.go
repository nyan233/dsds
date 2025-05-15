package btree

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"strconv"
	"sync"
	"testing"
)

func TestBTree(t *testing.T) {
	os.Remove("test/zbh.db")
	os.Remove("test/zbh.alog")
	bt := NewBTree("test", "zbh", 1000)
	err := bt.Init()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10000; i++ {
		err := bt.Insert(strconv.Itoa(i), []byte("Hello-World"))
		if err != nil {
			t.Fatal(err)
		}
	}
	t.Log(bt.Search("5000"))
}

func TestParInsert(t *testing.T) {
	type testCase struct {
		Group string
		Name  string
		bt    *BTree
	}
	cases := make([]testCase, 0, 5)
	for i := 0; i < cap(cases); i++ {
		var err error
		os.Remove(fmt.Sprintf("multi.%d.dat", i))
		os.Remove(fmt.Sprintf("multi.%d.bitmap", i))
		cases = append(cases, testCase{
			Group: "test",
			Name:  "multi." + strconv.Itoa(i),
		})
		cases[i].bt = NewBTree(cases[i].Group, cases[i].Name, 1000)
		err = cases[i].bt.Init()
		assert.Nil(t, err, "bt.Init() error")
	}
	wg := &sync.WaitGroup{}
	wg.Add(len(cases))
	for _, c := range cases {
		go func(bt *BTree) {
			defer wg.Done()
			for i := 0; i < 100000; i++ {
				err := bt.Insert(strconv.Itoa(i), []byte("Hello-World"))
				if err != nil {
					t.Fatal(err)
				}
			}
		}(c.bt)
	}
	wg.Wait()
}

func BenchmarkBTreeSearch(b *testing.B) {
	os.Remove("test/zbh.db")
	os.Remove("test/zbh.alog")
	bt := NewBTree("test", "zbh", 1000)
	err := bt.Init()
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 100000; i++ {
		err := bt.Insert(strconv.Itoa(i), []byte("Hello-World"))
		if err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _, err := bt.Search("89999")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestSetFlags(t *testing.T) {
	os.Remove("test/btnode.bitmap")
	os.Remove("test/btnode.alog")
	bt := NewBTree("test", "btnode", 1000)
	err := bt.Init()
	if err != nil {
		t.Fatal(err)
	}
	node1, err := bt.nodeMgr.createNewNode()
	if err != nil {
		t.Fatal(err)
	}
	node1.SetLeaf(true)
	assert.Equal(t, nil, node1.syncConfig())
	assert.Equal(t, nil, node1.load())
	assert.Equal(t, true, node1.IsLeaf())
	node1.SetLeaf(false)
	assert.Equal(t, nil, node1.syncConfig())
	assert.Equal(t, nil, node1.load())
	assert.Equal(t, false, node1.IsLeaf())
}

func TestBTreeNode(t *testing.T) {
	const EntryN = 100000
	os.Remove("test/btnode.bitmap")
	os.Remove("test/btnode.alog")
	bt := NewBTree("test", "btnode", 1000)
	err := bt.Init()
	if err != nil {
		t.Fatal(err)
	}
	node1, err := bt.nodeMgr.createNewNode()
	if err != nil {
		t.Fatal(err)
	}
	node2, err := bt.nodeMgr.createNewNode()
	if err != nil {
		t.Fatal(err)
	}
	entries := make([]Entry, 0, EntryN)
	for i := 0; i < EntryN; i++ {
		entries = append(entries, Entry{
			Key:   strconv.Itoa(i),
			Value: []byte("Hello-World"),
		})
	}
	//err = node1.appendEntry(entries)
	//if err != nil {
	//	return
	//}
	node1.allocAndWriteEntrySpace(entries)
	node1.entries = append(node1.entries, entries...)
	node1.syncEntries()
	node1.SetLeaf(true)
	assert.Equal(t, nil, node1.syncConfig())
	node1Flags := node1.flags
	node1Entries := node1.entries
	err = node1.copyTo(node2, true)
	if err != nil {
		t.Fatal(err)
	}
	if !node2.isLoad {
		err = node2.load()
		if err != nil {
			t.Fatal(err)
		}
	}
	assert.Equal(t, node1Flags, node2.flags)
	for k, v := range node2.entries {
		assert.Equal(t, node1Entries[k], v)
	}
}
