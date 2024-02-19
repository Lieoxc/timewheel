package timewheel

import (
	"container/list"
	"sync"
	"time"
)

type TimeWheel struct {
	sync.Once                             // 单例工具
	interval     time.Duration            // 轮询时间间隔
	ticker       time.Ticker              // 滴答定时器
	stopc        chan struct{}            // 关闭时间轮信号
	slots        []*list.List             // 时间轮数组
	curSlot      int                      // 当前轮询位置
	keyToTask    map[string]*list.Element //任务key到 节点的映射
	addTaskCh    chan *taskElement        // 添加定时任务
	removeTaskCh chan string              // 删除定时任务
}

type taskElement struct {
	task  func()
	pos   int    // 定时任务挂载在 slots 数组的索引位置
	key   string // 定时任务唯一标识
	cycle int    // 定时任务的延迟轮次，当cycle为 0表示当前轮次执行
}

func NewTimeWheel(slotNum int, interval time.Duration) *TimeWheel {
	if slotNum <= 0 {
		slotNum = 60
	}
	if interval <= 0 {
		interval = time.Second
	}
	t := &TimeWheel{
		interval:     interval,
		ticker:       *time.NewTicker(interval),
		stopc:        make(chan struct{}),
		slots:        make([]*list.List, 0, slotNum),
		keyToTask:    make(map[string]*list.Element),
		addTaskCh:    make(chan *taskElement),
		removeTaskCh: make(chan string),
	}
	// 给每一个槽 初始化一个链表
	for i := 0; i < slotNum; i++ {
		t.slots = append(t.slots, list.New())
	}
	go t.Run()
	return t
}

func (t *TimeWheel) Run() {
	for {
		select {
		case <-t.stopc:
			return
		case task := <-t.addTaskCh:
			t.addTask(task)
		case removeKey := <-t.removeTaskCh:
			t.removeTask(removeKey)
		case <-t.ticker.C:
			t.tick()
		}
	}
}
func (t *TimeWheel) Stop() {
	t.Do(
		func() {
			t.ticker.Stop() // 定制定时器 ticker
			close(t.stopc)  // 关闭定时器运行的 stopc
		})
}

// 外部调用新增一个定时任务
func (t *TimeWheel) AddTask(key string, task func(), execute time.Time) {
	pos, cycle := t.getPosAndCycle(execute)
	t.addTaskCh <- &taskElement{
		task:  task,
		pos:   pos,
		cycle: cycle,
		key:   key,
	}
}

func (t *TimeWheel) getPosAndCycle(execute time.Time) (int, int) {
	delay := int(time.Until(execute))
	// 计算出轮次
	cycle := delay / (len(t.slots) * int(t.interval))         // 总的延迟时间 / 一轮所需时间
	pos := (t.curSlot + delay/int(t.interval)) % len(t.slots) // (当前刻度 + 总共需要偏移的刻度 ) % 槽的数量
	return pos, cycle
}

func (t *TimeWheel) addTask(task *taskElement) {
	// 找到所需要添加的槽
	list := t.slots[task.pos]
	// 倘若定时任务 key 之前已存在，则需要先删除定时任务
	if _, ok := t.keyToTask[task.key]; ok {
		t.removeTask(task.key)
	}

	elemTask := list.PushBack(task)  //把这个任务节点新增到链表尾部
	t.keyToTask[task.key] = elemTask // 将key 与任务节点的指针做一个映射
}

// 删除任务
func (t *TimeWheel) RemoveTask(key string) {
	t.removeTaskCh <- key
}
func (t *TimeWheel) removeTask(removeKey string) {
	if elem, ok := t.keyToTask[removeKey]; ok {
		delete(t.keyToTask, removeKey)
		task, _ := elem.Value.(*taskElement) // 取出节点信息
		t.slots[task.pos].Remove(elem)       // 根据节点信息，定位出槽的索引，然后从链表删除该节点
	}
}

// 执行任务
func (t *TimeWheel) tick() {
	list := t.slots[t.curSlot]
	defer func() {
		// curSlot 往后移一位  之所以需要 % len(t.slots) 是为了满足环形遍历
		t.curSlot = (t.curSlot + 1) % len(t.slots)
	}()
	t.execute(list)
}
func (t *TimeWheel) execute(l *list.List) {
	// 遍历list
	for e := l.Front(); e != nil; {
		taskElement, _ := e.Value.(*taskElement)
		if taskElement.cycle > 0 {
			taskElement.cycle--
			e = e.Next()
			continue
		}
		// 已经到达执行条件
		go taskElement.task()

		next := e.Next() // 临时先保存 e.Next
		l.Remove(e)
		delete(t.keyToTask, taskElement.key)
		e = next //  开始检查链表下一个节点
	}
}
