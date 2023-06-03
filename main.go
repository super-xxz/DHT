package main

import (
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"math/big"
)

/**
 * @author xxz
 * @date 2023-06-03
 **/

type Node struct {
	nodeID  string
	buckets []*Bucket
	keys    map[string][]byte
}

type Bucket struct {
	ids []string
}

var nodesMap map[string]*Node

var SetNodes []string
var GetNodes []string

// FindNode 根据节点ID查找最近的节点
func (s *Node) FindNode(nodeID string, array []string) []string {
	var nodes []string
	var return_node []string
	if s.nodeID == nodeID {
		nodes = append(nodes, s.nodeID, s.nodeID)
		return nodes
	}

	if len(s.buckets) == 0 {
		s.InsertNode(nodeID)
		return array
	}

	// 寻找到对应的桶
	result := findBucket(s.nodeID, nodeID)
	var bucket *Bucket
	if result >= (len(s.buckets) - 1) {
		bucket = s.buckets[len(s.buckets)-1]
	} else {
		bucket = s.buckets[result]
	}

	// 判断桶中是否存在该节点
	for _, v := range bucket.ids {
		if v == nodeID {
			nodes = append(nodes, v, v)
			return nodes
		}
	}

	var node1, node2 string
	var nodeNum int
	// 不存在就进行递归，桶中选取随机的两个节点
	if len(bucket.ids) == 2 {
		node1 = bucket.ids[0]
		node2 = bucket.ids[1]
		nodeNum = 2
	} else if len(bucket.ids) == 1 {
		node1 = bucket.ids[0]
		nodeNum = 1
	} else if len(bucket.ids) > 2 {
		index1, index2 := GetRandom2()
		node1 = bucket.ids[index1]
		node2 = bucket.ids[index2]
		nodeNum = 2
	}

	// 判断两个新选取的节点的距离与传入的节点的距离相比
	// 如果找不到比传入节点更近的节点，寻找就结束（找不到比传入更近的）
	// 如果找到的话执行FindNode，对更新的节点进行查找
	if nodeNum == 2 {
		// 在第一遍比较的时候，对于array中已经发生交换的元素，在第二次比较的时候就会进行跳过
		isUpdate := -1
		for i, v := range array {
			result := compareGetMin(nodeID, v, node1)
			if result == node1 {
				array[i] = node1
				isUpdate = i
				return_node = append(return_node, nodesMap[node1].FindNode(nodeID, array)...)
			}
		}

		for i := len(array) - 1; i >= 0; i-- {
			if i == isUpdate {
				continue
			}
			result := compareGetMin(nodeID, array[i], node2)
			if result == node2 {
				array[i] = node2
				return_node = append(return_node, nodesMap[node2].FindNode(nodeID, array)...)
			}
		}
	} else if nodeNum == 1 {
		num := new(big.Int)
		num.SetString(nodeID, 10)
		for i := len(array) - 1; i >= 0; i-- {
			num1 := new(big.Int)
			num1.SetString(array[i], 10)
			result := num1.Cmp(num)
			if result == 1 {
				array[i] = node1
				return_node = append(return_node, nodesMap[node1].FindNode(nodeID, array)...)
			}
		}
	}

	return return_node
}

// InsertNode 插入新节点
func (s *Node) InsertNode(nodeID string) {
	if len(s.buckets) == 0 {
		bucket := new(Bucket)
		bucket.ids = append(bucket.ids, nodeID)
		s.buckets = append(s.buckets, bucket)
		return
	}

	bucket_index := findBucket(s.nodeID, nodeID)
	bucket := s.buckets[bucket_index]

	// 桶未满，直接插入新节点
	if len(bucket.ids) < 3 {
		bucket.ids = append(bucket.ids, nodeID)
		return
	}

	// 桶已满，进行分裂
	new_bucket := new(Bucket)
	new_bucket.ids = append(new_bucket.ids, bucket.ids[2])

	bucket.ids = bucket.ids[:2]

	s.buckets = append(s.buckets, nil)
	copy(s.buckets[bucket_index+2:], s.buckets[bucket_index+1:])
	s.buckets[bucket_index+1] = new_bucket

	// 桶分裂后，重新判断新节点的插入位置
	s.InsertNode(nodeID)
}

// SetValue 设置键值对
func (s *Node) SetValue(key string, value []byte) bool {
	key_hash := hash(key)
	if key_hash == hash(s.nodeID) {
		s.keys[key] = value
		return true
	}

	if len(s.buckets) == 0 {
		return false
	}

	result := findBucket(s.nodeID, key_hash)
	var bucket *Bucket
	if result >= (len(s.buckets) - 1) {
		bucket = s.buckets[len(s.buckets)-1]
	} else {
		bucket = s.buckets[result]
	}

	for _, v := range bucket.ids {
		if v == s.nodeID {
			s.keys[key] = value
			return true
		}
	}

	// 找到最近的两个节点并传递键值对
	node1 := bucket.ids[0]
	node2 := bucket.ids[1]

	if node1 == s.nodeID {
		nodesMap[node2].SetValue(key, value)
	} else {
		nodesMap[node1].SetValue(key, value)
	}

	return true
}

// GetValue 获取键对应的值
func (s *Node) GetValue(key string) []byte {
	if value, ok := s.keys[key]; ok {
		return value
	}

	key_hash := hash(key)

	if len(s.buckets) == 0 {
		return nil
	}

	result := findBucket(s.nodeID, key_hash)
	var bucket *Bucket
	if result >= (len(s.buckets) - 1) {
		bucket = s.buckets[len(s.buckets)-1]
	} else {
		bucket = s.buckets[result]
	}

	for _, v := range bucket.ids {
		if v == s.nodeID {
			return nil
		}
	}

	node1 := bucket.ids[0]
	node2 := bucket.ids[1]

	var value []byte
	if node1 == s.nodeID {
		value = nodesMap[node2].GetValue(key)
	} else {
		value = nodesMap[node1].GetValue(key)
	}

	return value
}

// compareGetMin 比较两个节点距离传入节点的距离，返回较近的节点
func compareGetMin(nodeID string, node1 string, node2 string) string {
	num1 := new(big.Int)
	num1.SetString(nodeID, 10)
	num2 := new(big.Int)
	num2.SetString(node1, 10)
	result1 := new(big.Int).Sub(num2, num1)
	num3 := new(big.Int)
	num3.SetString(nodeID, 10)
	num4 := new(big.Int)
	num4.SetString(node2, 10)
	result2 := new(big.Int).Sub(num4, num3)

	if result1.Sign() < 0 {
		result1 = result1.Neg(result1)
	}

	if result2.Sign() < 0 {
		result2 = result2.Neg(result2)
	}

	compare := result1.Cmp(result2)
	if compare == -1 {
		return node1
	} else {
		return node2
	}
}

// GetRandom2 生成两个不同的随机数
func GetRandom2() (int, int) {
	max := 1
	min := 0
	index1 := randInt(min, max)
	index2 := randInt(min, max)

	for index1 == index2 {
		index2 = randInt(min, max)
	}

	return index1, index2
}

// randInt 生成[min, max]之间的随机整数
func randInt(min, max int) int {
	if min > max {
		panic("max must be greater than min")
	}

	result, err := rand.Int(rand.Reader, big.NewInt(int64(max-min+1)))
	if err != nil {
		panic(err)
	}

	return int(result.Int64()) + min
}

// findBucket 查找节点所在的桶
func findBucket(nodeID string, target string) int {
	num1 := new(big.Int)
	num1.SetString(nodeID, 10)
	num2 := new(big.Int)
	num2.SetString(target, 10)
	result := new(big.Int).Xor(num1, num2)
	hex_str := hex.EncodeToString(result.Bytes())
	bucket_index, err := hex.DecodeString(hex_str[len(hex_str)-1:])
	if err != nil {
		panic(err)
	}

	return int(bucket_index[0])
}

// hash 计算SHA1哈希值并进行Base64编码
func hash(s string) string {
	hasher := sha1.New()
	hasher.Write([]byte(s))
	sha := base64.URLEncoding.EncodeToString(hasher.Sum(nil))
	return sha
}

func main() {
	nodesMap = make(map[string]*Node)

	node1 := Node{
		nodeID: "1",
		keys:   make(map[string][]byte),
	}

	node2 := Node{
		nodeID: "2",
		keys:   make(map[string][]byte),
	}

	node3 := Node{
		nodeID: "3",
		keys:   make(map[string][]byte),
	}

	nodesMap["1"] = &node1
	nodesMap["2"] = &node2
	nodesMap["3"] = &node3

	SetNodes = append(SetNodes, "1", "2", "3")
	GetNodes = append(GetNodes, "1", "2", "3")

	node1.InsertNode("2")
	node2.InsertNode("3")
	node3.InsertNode("1")

	fmt.Println("节点1的桶：", node1.buckets)
	fmt.Println("节点2的桶：", node2.buckets)
	fmt.Println("节点3的桶：", node3.buckets)

	fmt.Println("节点1插入节点2后的桶：", node1.buckets)
	fmt.Println("节点2插入节点3后的桶：", node2.buckets)
	fmt.Println("节点3插入节点1后的桶：", node3.buckets)

	node1.SetValue("key1", []byte("value1"))
	node2.SetValue("key2", []byte("value2"))
	node3.SetValue("key3", []byte("value3"))

	value1 := node1.GetValue("key1")
	value2 := node2.GetValue("key2")
	value3 := node3.GetValue("key3")

	fmt.Println("节点1的键值对：", node1.keys)
	fmt.Println("节点2的键值对：", node2.keys)
	fmt.Println("节点3的键值对：", node3.keys)

	fmt.Println("节点1获取key1的值：", string(value1))
	fmt.Println("节点2获取key2的值：", string(value2))
	fmt.Println("节点3获取key3的值：", string(value3))

	find_nodes := node1.FindNode("2", SetNodes)
	fmt.Println("节点2所在的最近节点：", find_nodes)
}
