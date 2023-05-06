package ketama

import (
	"fmt"
	"hash/crc32"
	"sort"
	"sync"
)

// Node represents a physical node in the cluster.
type Node struct {
	ID     string      // Node ID
	Weight int         // Node weight
	Value  interface{} // Additional value to be stored
}

// Ketama represents a ketama ketama.
type Ketama struct {
	mu           sync.RWMutex
	sortedKeys   []uint32
	nodeHashMap  map[uint32]Node
	virtualNodes int
}

// NewKetama creates a new Ketama ketama.
func NewKetama(nodes []Node, virtualNodes int) *Ketama {
	ketama := &Ketama{
		nodeHashMap:  make(map[uint32]Node),
		virtualNodes: virtualNodes,
	}

	for _, node := range nodes {
		ketama.addNode(node)
	}

	return ketama
}

// AddNode adds a node to the ketama.
func (k *Ketama) AddNode(node Node) {
	k.mu.Lock()
	defer k.mu.Unlock()

	k.addNode(node)
}

// addNode adds a node to the ketama.
func (k *Ketama) addNode(node Node) {
	for i := 0; i < k.virtualNodes; i++ {
		nodeKey := generateNodeKey(node.ID, i)
		k.nodeHashMap[nodeKey] = node
		k.sortedKeys = append(k.sortedKeys, nodeKey)
	}

	sort.Slice(k.sortedKeys, func(i, j int) bool {
		return k.sortedKeys[i] < k.sortedKeys[j]
	})
}

// RemoveNode removes a node from the ketama.
func (k *Ketama) RemoveNode(nodeID string) {
	k.mu.Lock()
	defer k.mu.Unlock()

	newSortedKeys := make([]uint32, 0)
	for _, key := range k.sortedKeys {
		node := k.nodeHashMap[key]
		if node.ID != nodeID {
			newSortedKeys = append(newSortedKeys, key)
		}
	}
	k.sortedKeys = newSortedKeys

	for i := 0; i < k.virtualNodes; i++ {
		nodeKey := generateNodeKey(nodeID, i)
		delete(k.nodeHashMap, nodeKey)
	}
}

// GetNode gets the node for the given key.
func (k *Ketama) GetNode(key string) (Node, bool) {
	k.mu.RLock()
	defer k.mu.RUnlock()

	if len(k.sortedKeys) == 0 {
		return Node{}, false
	}

	keyHash := crc32.ChecksumIEEE([]byte(key))
	idx := sort.Search(len(k.sortedKeys), func(i int) bool {
		return k.sortedKeys[i] >= keyHash
	})

	if idx == len(k.sortedKeys) {
		idx = 0
	}

	node := k.nodeHashMap[k.sortedKeys[idx]]
	return node, true
}

// generateNodeKey generates a key for the virtual node.
func generateNodeKey(nodeID string, i int) uint32 {
	key := fmt.Sprintf("%s-%d", nodeID, i)
	return crc32.ChecksumIEEE([]byte(key))
}
