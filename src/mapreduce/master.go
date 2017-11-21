package mapreduce

//
// Please do not modify this file.
//

import (
	"fmt"
	"net"
	"sync"
)

// Master 会维持所有 Master 需要追踪的状态信息
type Master struct {
	sync.Mutex

	address     string
	doneChannel chan bool

	// 由互斥锁所保护的 Master 状态
	newCond *sync.Cond // 当新的 Worker 被 Register 方法加入到 workers[] 中时发出信号
	workers []string   // 每个 Worker 的 UNIX 域套接字名称，即其 RPC 地址

	// 任务信息
	jobName string   // 当前作业的名称
	files   []string // 输入文件
	nReduce int      // Reduce 分片的数量

	shutdown chan struct{}
	l        net.Listener
	stats    []int
}

// Register 是一个供 Worker 调用的 RPC 方法，用于通知 Master 它们已经准备好接收任务
func (mr *Master) Register(args *RegisterArgs, _ *struct{}) error {
	mr.Lock()
	defer mr.Unlock()
	debug("Register: worker %s\n", args.Worker)
	mr.workers = append(mr.workers, args.Worker)

	// 通知 forwardRegistrations() 函数 workers[] 有了新的元素
	mr.newCond.Broadcast()

	return nil
}

// newMaster 初始化一个新的 Map/Reduce Master
func newMaster(master string) (mr *Master) {
	mr = new(Master)
	mr.address = master
	mr.shutdown = make(chan struct{})
	mr.newCond = sync.NewCond(mr)
	mr.doneChannel = make(chan bool)
	return
}

// Sequential 会顺序地运行 Map 和 Reduce 任务，在开始下一个任务前等待上一个任务的完成
func Sequential(jobName string, files []string, nReduce int,
	mapF func(string, string) []KeyValue,
	reduceF func(string, []string) string,
) (mr *Master) {
	mr = newMaster("master")
	go mr.run(jobName, files, nReduce, func(phase jobPhase) {
		switch phase {
		case mapPhase:
			for i, f := range mr.files {
				doMap(mr.jobName, i, f, mr.nReduce, mapF)
			}
		case reducePhase:
			for i := 0; i < mr.nReduce; i++ {
				doReduce(mr.jobName, i, mergeName(mr.jobName, i), len(mr.files), reduceF)
			}
		}
	}, func() {
		mr.stats = []int{len(files) + nReduce}
	})
	return
}

// forwardRegistrations 会把所以已注册和新注册的 Worker 信息发送到给定的 Channel 中。
// schedule() 会从该 Channel 中获知 Worker
func (mr *Master) forwardRegistrations(ch chan string) {
	i := 0
	for {
		mr.Lock()
		if len(mr.workers) > i {
			// 发现了一个我们还没有告知 schedule() 的 Worker
			w := mr.workers[i]
			go func() { ch <- w }() // 在互斥锁外完成信息发送
			i = i + 1
		} else {
			// 等待 Register() 将新的 Worker 信息添加到 workers[] 中
			mr.newCond.Wait()
		}
		mr.Unlock()
	}
}

// Distributed 会通过 RPC 将 Map 和 Reduce 任务调度到已注册的 Worker 上
func Distributed(jobName string, files []string, nReduce int, master string) (mr *Master) {
	mr = newMaster(master)
	mr.startRPCServer()
	go mr.run(jobName, files, nReduce,
		func(phase jobPhase) {
			ch := make(chan string)
			go mr.forwardRegistrations(ch)
			schedule(mr.jobName, mr.files, mr.nReduce, phase, ch)
		},
		func() {
			mr.stats = mr.killWorkers()
			mr.stopRPCServer()
		})
	return
}

// run 会在给定数量的 Mapper 和 Reducer 上执行指定的 MapReduce 作业
//
// 首先它会按照给定的 Mapper 数量切分给定的输入文件，并把每个任务调度到空闲的 Worker 上。
// 每个 Map 任务都会把它的输出内容放入到与 Reduce 任务数量等同的文件中。等所有 Mapper
// 都完成后，Worker 就会被调度执行 Reduce 任务。
//
// 当所有任务都完成后，Reducer 的输出会被合并，其他统计信息会被收集，然后 Master 关闭
//
// 注意该实现假设结点运行在一个共享的文件系统之上。
func (mr *Master) run(jobName string, files []string, nReduce int,
	schedule func(phase jobPhase),
	finish func(),
) {
	mr.jobName = jobName
	mr.files = files
	mr.nReduce = nReduce

	fmt.Printf("%s: Starting Map/Reduce task %s\n", mr.address, mr.jobName)

	schedule(mapPhase)
	schedule(reducePhase)
	finish()
	mr.merge()

	fmt.Printf("%s: Map/Reduce task completed\n", mr.address)

	mr.doneChannel <- true
}

// Wait 会一直阻塞直至当前调度的作业已完成
func (mr *Master) Wait() {
	<-mr.doneChannel
}

// killWorkers 会通过对每个 Worker 发送关闭 RPC 信号以清理 Worker。它还会收集并返回每个 Worker
// 已经完成的任务数
func (mr *Master) killWorkers() []int {
	mr.Lock()
	defer mr.Unlock()
	nTasks := make([]int, 0, len(mr.workers))
	for _, w := range mr.workers {
		debug("Master: shutdown worker %s\n", w)
		var reply ShutdownReply
		ok := call(w, "Worker.Shutdown", new(struct{}), &reply)
		if ok == false {
			fmt.Printf("Master: RPC %s shutdown error\n", w)
		} else {
			nTasks = append(nTasks, reply.NTasks)
		}
	}
	return nTasks
}
