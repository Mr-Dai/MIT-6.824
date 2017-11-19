package mapreduce

//
// 不要修改这个文件
//

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
)

// Worker 会保存一个等待 DoTask 和 Shutdown RPC 调用的服务器的状态信息
type Worker struct {
	sync.Mutex

	name       string
	Map        func(string, string) []KeyValue
	Reduce     func(string, []string) string
	nRPC       int // 在这个数量的 RPC 调用后退出。由 mutex 所保护
	nTasks     int // 执行的总任务数。由 mutex 所保护
	concurrent int // 该 Worker 当前并发 DoTask 的数量。由 mutex 所保护
	l          net.Listener
}

// DoTask 会在一个新任务被分配到该 Worker 上时被 Master 调用
func (wk *Worker) DoTask(arg *DoTaskArgs, _ *struct{}) error {
	fmt.Printf("%s: given %v task #%d on file %s (nios: %d)\n",
		wk.name, arg.Phase, arg.TaskNumber, arg.File, arg.NumOtherPhase)

	wk.Lock()
	wk.nTasks += 1
	wk.concurrent += 1
	nc := wk.concurrent
	wk.Unlock()

	if nc > 1 {
		// schedule() should never issue more than one RPC at a
		// time to a given worker.
		log.Fatal("Worker.DoTask: more than one DoTask sent concurrently to a single worker\n")
	}

	switch arg.Phase {
	case mapPhase:
		doMap(arg.JobName, arg.TaskNumber, arg.File, arg.NumOtherPhase, wk.Map)
	case reducePhase:
		doReduce(arg.JobName, arg.TaskNumber, mergeName(arg.JobName, arg.TaskNumber), arg.NumOtherPhase, wk.Reduce)
	}

	wk.Lock()
	wk.concurrent -= 1
	wk.Unlock()

	fmt.Printf("%s: %v task #%d done\n", wk.name, arg.Phase, arg.TaskNumber)
	return nil
}

// Shutdown 会在所有工作都完成后被 Master 调用。
// 我们应该返回 Worker 已处理的任务数量
func (wk *Worker) Shutdown(_ *struct{}, res *ShutdownReply) error {
	debug("Shutdown %s\n", wk.name)
	wk.Lock()
	defer wk.Unlock()
	res.NTasks = wk.nTasks
	wk.nRPC = 1
	return nil
}

// 告诉 Master Worker 准备好工作了
func (wk *Worker) register(master string) {
	args := new(RegisterArgs)
	args.Worker = wk.name
	ok := call(master, "Master.Register", args, new(struct{}))
	if ok == false {
		fmt.Printf("Register: RPC %s register error\n", master)
	}
}

// RunWorker 会启动一个与 Master 的连接，注册自己的地址，并等待其分配新的任务
func RunWorker(MasterAddress string, me string,
	MapFunc func(string, string) []KeyValue,
	ReduceFunc func(string, []string) string,
	nRPC int,
) {
	debug("RunWorker %s\n", me)
	wk := new(Worker)
	wk.name = me
	wk.Map = MapFunc
	wk.Reduce = ReduceFunc
	wk.nRPC = nRPC
	rpcs := rpc.NewServer()
	rpcs.Register(wk)
	os.Remove(me) // 只有使用 "unix" 协议时才需要
	l, e := net.Listen("unix", me)
	if e != nil {
		log.Fatal("RunWorker: worker ", me, " error: ", e)
	}
	wk.l = l
	wk.register(MasterAddress)

	// 不要修改下面的代码
	for {
		wk.Lock()
		if wk.nRPC == 0 {
			wk.Unlock()
			break
		}
		wk.Unlock()
		conn, err := wk.l.Accept()
		if err == nil {
			wk.Lock()
			wk.nRPC--
			wk.Unlock()
			go rpcs.ServeConn(conn)
		} else {
			break
		}
	}
	wk.l.Close()
	debug("RunWorker %s exit\n", me)
}
