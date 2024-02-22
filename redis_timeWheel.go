package timewheel

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/demdxx/gocast"
	"github.com/go-redis/redis/v7"
)

type RTimeWheel struct {
	sync.Once
	redisClient *redis.Client
	ticker      time.Ticker   // 滴答定时器
	stopc       chan struct{} // 关闭时间轮信号
}
type RTaskElement struct {
	// 定时任务全局唯一 key
	Key string `json:"key"`
	// 定时任务执行时，需要的数据
	Data string `json:"data"`
}

var (
	addTaskScript = `
	local zsetKey = KEYS[1]
	local deleteSetKey = KEYS[2]
	local score = ARGV[1]
	local task = ARGV[2]
	local taskKey = ARGV[3]
	-- 每次添加定时任务时，都直接将其从已删除任务 set 中移除，不管之前是否在 set 中
	redis.call('srem',deleteSetKey,taskKey)
	-- 调用 zadd 指令，将定时任务添加到 zset 中
	return redis.call('zadd',zsetKey,score,task)
 `
	delTaskScript = `
	local deleteSetKey = KEYS[1]
	local taskKey = ARGV[1]
	-- 将定时任务唯一键添加到 set 中
	redis.call('sadd',deleteSetKey,taskKey)
	local scnt = redis.call('scard',deleteSetKey)
	if (tonumber(scnt) == 1)
	then
		redis.call('expire',deleteSetKey,120)
	end
	return scnt
 `
	// 3 执行任务时，通过 zrange 操作取回所有不存在删除 key 标识的任务
	LuaZrangeTasks = `
	local zsetKey = KEYS[1]
	local deleteSetKey = KEYS[2]
	local score1 = ARGV[1]
	local score2 = ARGV[2]
	-- 获取到已删除任务的集合
	local deleteSet = redis.call('smembers',deleteSetKey)
	-- 根据秒级时间戳对 zset 进行 zrange 检索，获取到满足时间条件的定时任务
	local targets = redis.call('zrange',zsetKey,score1,score2,'byscore')
	-- 检索到的定时任务直接从时间轮中移除，保证分布式场景下定时任务不被重复获取
	redis.call('zremrangebyscore',zsetKey,score1,score2)
	-- lua的返回是table
	local reply = {}
	reply[1] = deleteSet

	for i, v in ipairs(targets) do
		reply[#reply+1]=v
	end
	return reply
`
)

const (
	YYYY_MM_DD_HH_MM = "2006-01-02-15:04"
)

// 外部创建一个时间轮
func NewRTimeWheel(redisClient *redis.Client) *RTimeWheel {
	r := &RTimeWheel{
		redisClient: redisClient,
		ticker:      *time.NewTicker(time.Second),
		stopc:       make(chan struct{}),
	}
	go r.Run()
	return r
}

// 后台协程
func (r *RTimeWheel) Run() {
	for {
		select {
		case <-r.stopc: //接收到退出信息
			return
		case <-r.ticker.C: // 滴答定时器发送信号
			go r.executeTasks()
		}
	}
}

// 外部调用停止 时间轮
func (r *RTimeWheel) Stop() {
	r.Do(func() {
		close(r.stopc)
		r.ticker.Stop()
	})
}

// 添加定时任务
func (r *RTimeWheel) AddTask(key string, task *RTaskElement, runTime time.Time) error {
	//
	task.Key = key
	taskBody, _ := json.Marshal(task)
	//
	err := r.redisClient.Eval(addTaskScript,
		[]string{
			r.getMinuteSlice(runTime),   // 定时任务有序集合中的 key
			r.getDeleteSetKey(runTime)}, // 待删除结合的 key
		[]interface{}{
			runTime.Unix(),   // 时间戳为有序结合的 对象分数
			string(taskBody), // 任务明细, 也就是有序集合中的 对象
			key,              // 任务 key，用于存放在删除集合中
		}).Err()
	return err
}

// 移除定时任务
func (r *RTimeWheel) RemoveTask(key string, runTime time.Time) error {
	err := r.redisClient.Eval(delTaskScript,
		[]string{r.getDeleteSetKey(runTime)}, // 待删除结合的 key
		[]interface{}{key}).Err()             // 待删除对象
	return err
}

func (r *RTimeWheel) executeTasks() {
	// 根据当前时间条件扫描redis 有序集合，获取满足执行条件的定时任务
	task, err := r.getExecutableTasks()
	if err != nil {
		return
	}
	var wg sync.WaitGroup
	for _, task := range task {
		wg.Add(1)
		Runtask := task
		go func() {
			defer func() {
				if err := recover(); err != nil {

				}
				wg.Done()
			}()
			//执行定时任务
			if err := r.executeTask(Runtask); err != nil {
				// do some thing
			}
		}()
	}
	wg.Wait() // 等待所有任务执行完成
}
func (r *RTimeWheel) executeTask(task *RTaskElement) error {
	// TODO do something
	return nil
}

func (r *RTimeWheel) getExecutableTasks() ([]*RTaskElement, error) {
	now := time.Now()
	minuteZsetVal := r.getMinuteSlice(now)
	deleteSetKey := r.getDeleteSetKey(now)
	nowSecond := getTimeSecond(now)
	score1 := nowSecond.Unix()
	score2 := nowSecond.Add(time.Second).Unix()
	rawReply, err := r.redisClient.Eval(LuaZrangeTasks,
		[]string{minuteZsetVal, deleteSetKey},  // 此时刻需要查询的有序列表key，以及应该删除的集合key
		[]interface{}{score1, score2}).Result() // 检索的上下限分数

	if err != nil {
		return nil, err
	}
	//lua脚本中首个返回元素为已删除任务的key 集合
	replies := gocast.ToInterfaceSlice(rawReply)
	if len(replies) == 0 {
		return nil, fmt.Errorf("invalid replies: %v", replies)
	}
	// deleteds 里面的元素就是调用 RemoveTask 的时候，移动到 deleteSetKey 这个集合里面的元素
	deleteds := gocast.ToStringSlice(replies[0])
	deletedSet := make(map[string]struct{}, len(deleteds))
	for _, deleted := range deleteds {
		deletedSet[deleted] = struct{}{}
	}
	// 遍历各个定时任务
	tasks := make([]*RTaskElement, 0, len(replies)-1)
	for i := 1; i < len(replies); i++ {
		var task RTaskElement
		if err := json.Unmarshal([]byte(gocast.ToString(replies[i])), &task); err != nil {
			// TODO add warning log
			continue
		}
		// 这个任务在待删除集合里面，所以过滤掉
		if _, ok := deletedSet[task.key]; ok {
			continue
		}
		tasks = append(tasks, &task)
	}
	return tasks, nil
}

// 获取redis有序集合表 里面对应的分钟key
func (r *RTimeWheel) getMinuteSlice(runTime time.Time) string {
	return fmt.Sprintf("timeWheel_task_{%s}", runTime.Format(YYYY_MM_DD_HH_MM))
}

// 获取redis有序集合表 里面对应的分钟key
func (r *RTimeWheel) getDeleteSetKey(runTime time.Time) string {
	return fmt.Sprintf("timeWheel_delSet_{%s}", runTime.Format(YYYY_MM_DD_HH_MM))
}

func getTimeSecond(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), 0, time.Local)
}
