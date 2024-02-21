package timewheel

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

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
	key string `json:"key"`
	// 定时任务执行时，回调的 http url
	CallbackURL string `json:"callback_url"`
	// 回调时使用的 http 方法
	Method string `json:"method"`
	// 回调时传递的请求参数
	Req interface{} `json:"req"`
	// 回调时使用的 http 请求头
	Header map[string]string `json:"header"`
}

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
)

const (
	YYYY_MM_DD_HH_MM = "2006-01-02-15:04"
)

// 添加定时任务
func (r *RTimeWheel) AddTask(key string, task *RTaskElement, runTime time.Time) error {
	//
	task.key = key
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

// 获取redis有序集合表 里面对应的分钟key
func (r *RTimeWheel) getMinuteSlice(runTime time.Time) string {
	return fmt.Sprintf("timeWheel_task_{%s}", runTime.Format(YYYY_MM_DD_HH_MM))
}

// 获取redis有序集合表 里面对应的分钟key
func (r *RTimeWheel) getDeleteSetKey(runTime time.Time) string {
	return fmt.Sprintf("timeWheel_delSet_{%s}", runTime.Format(YYYY_MM_DD_HH_MM))
}

func (r *RTimeWheel) executeTasks() {

}
