package heapmap

import (
	"fmt"
	"sync"
)

type Node struct {
	Priority int
	Key      string
}

func (s *Node) String() string {
	return fmt.Sprintf("(%s, %d)", s.Key, s.Priority)
}

type HeapMap interface {
	GetTopNodes(int) ([]*Node, error)
	GetPriority(string) (int, error)
	Insert(string, int) error
	Top() (*Node, error)
}

type heapMap struct {
	heap []*Node
	hash map[string]*Node
	size int
	mu   sync.RWMutex
}

func NewHeapMap(size int) HeapMap {
	return &heapMap{
		heap: make([]*Node, size),
		hash: make(map[string]*Node, size),
	}
}

func (hm *heapMap) adjustHeap(pos, max int) {
	left := 2*pos + 1
	right := 2*pos + 2
	if left < max && right < max {
		var maxIdx int
		if hm.heap[left].Priority > hm.heap[right].Priority {
			maxIdx = left
		} else {
			maxIdx = right
		}
		if hm.heap[maxIdx].Priority > hm.heap[pos].Priority {
			hm.heap[pos], hm.heap[maxIdx] = hm.heap[maxIdx], hm.heap[pos]
			hm.adjustHeap(maxIdx, max)
		}
	} else if left < max {
		maxIdx := left
		if hm.heap[maxIdx].Priority > hm.heap[pos].Priority {
			hm.heap[pos], hm.heap[maxIdx] = hm.heap[maxIdx], hm.heap[pos]
			hm.adjustHeap(maxIdx, max)
		}
	}
}

func (hm *heapMap) Insert(key string, priority int) error {
	hm.mu.Lock()
	defer hm.mu.Unlock()
	if node, ok := hm.hash[key]; ok {
		node.Priority += priority
		hm.adjustHeap(0, hm.size)
		return nil
	}
	if hm.size == (len(hm.heap) - 1) {
		return fmt.Errorf("Error: heap full")
	}
	node := &Node{
		Key:      key,
		Priority: priority,
	}
	hm.insert(node)
	hm.hash[key] = node
	return nil
}

// warning, lock externally before using
func (hm *heapMap) insert(node *Node) {
	hm.heap[hm.size] = node
	cur := hm.size
	parent := (cur - 1) / 2
	for cur >= 0 && hm.heap[cur].Priority > hm.heap[parent].Priority {
		hm.heap[cur], hm.heap[parent] = hm.heap[parent], hm.heap[cur]
		cur = parent
		parent = (cur - 1) / 2
	}
	hm.size++
}

// warning, lock externally before using
func (hm *heapMap) pop() (*Node, error) {
	if hm.size == 0 {
		return nil, fmt.Errorf("Error: heap empty")
	}
	temp := hm.heap[0]
	hm.heap[0] = hm.heap[hm.size-1]
	hm.size--
	hm.adjustHeap(0, hm.size)
	return temp, nil
}

func (hm *heapMap) GetTopNodes(numNodess int) ([]*Node, error) {
	hm.mu.Lock()
	defer hm.mu.Unlock()
	temp := []*Node{}
	var (
		err  error
		node *Node
	)
	for i := 0; i < numNodess; i++ {
		if node, err = hm.pop(); err == nil {
			temp = append(temp, node)
		} else {
			break
		}
	}
	for i := len(temp) - 1; i >= 0; i-- {
		hm.insert(temp[i])
	}
	return temp, err
}

func (hm *heapMap) GetPriority(key string) (int, error) {
	hm.mu.RLock()
	defer hm.mu.RUnlock()
	if node, ok := hm.hash[key]; ok {
		return node.Priority, nil
	}
	return 0, fmt.Errorf("Key: %s not in heap", key)
}

func (hm *heapMap) Top() (*Node, error) {
	hm.mu.RLock()
	defer hm.mu.RUnlock()
	if hm.size == 0 {
		return nil, fmt.Errorf("Error: heap empty")
	}
	return hm.heap[0], nil
}
