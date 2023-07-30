package mr

type TaskData struct {
	pid     int
	startTs int64
	taskId  int
}

type Node struct {
	data TaskData
	next *Node
	prev *Node
}

type DoublyLinkedList struct {
	head *Node
	tail *Node
	size int
	mapp map[int]*Node
}

func NewDoublyLinkedList() *DoublyLinkedList {
	return &DoublyLinkedList{
		head: nil,
		tail: nil,
		size: 0,
		mapp: make(map[int]*Node),
	}
}

func (dll *DoublyLinkedList) AddNode(data TaskData) {

	newNode := &Node{
		data: data,
		next: nil,
		prev: nil,
	}

	if dll.head == nil {
		dll.head = newNode
		dll.tail = newNode
	} else {
		dll.tail.next = newNode
		newNode.prev = dll.tail
		dll.tail = newNode
	}

	dll.size++
	dll.mapp[data.pid] = newNode
}

func (dll *DoublyLinkedList) GetLastNodeValue() *TaskData {

	if dll.head == nil {
		return nil
	}
	return &dll.head.data
}

func (dll *DoublyLinkedList) RemoveNode(pid int) {

	node, exists := dll.mapp[pid]

	if !exists {
		return
	}

	if node == dll.head {
		dll.head = node.next
	} else if node == dll.tail {
		dll.tail = node.prev
		dll.tail.next = nil
	} else {
		node.prev.next = node.next
		node.next.prev = node.prev
	}

	delete(dll.mapp, pid)
	dll.size--
}

func (dll *DoublyLinkedList) GetSize() int {
	return dll.size
}
