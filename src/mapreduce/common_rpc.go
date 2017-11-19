package mapreduce

import (
	"fmt"
	"net/rpc"
)

// 以下这些都是 RPC 相关的类型和方法
// 字段名必须与大写字母开头，否则 RPC 将无法工作

// DoTaskArgs 会在作业被调度到 Worker 上时保存传给 Worker 的参数
type DoTaskArgs struct {
	JobName    string
	File       string   // 只被用于 Map，输入文件
	Phase      jobPhase // 现在是 Map 还是 Reduce
	TaskNumber int      // 该任务在该阶段中的索引值

	// NumOtherPhase 指另一个阶段的任务总数。Mapper 需要用该值计算输出文件的数量，
	// 而 Reduce 需要利用该值得知具体要读入多少个文件
	NumOtherPhase int
}

// ShutdownReply 是对 WorkerShutdown 的响应。
// 它包含了 Worker 自启动以来已执行的任务数
type ShutdownReply struct {
	NTasks int
}

// RegisterArgs 是 Worker 向 Master 注册时传递的参数
type RegisterArgs struct {
	Worker string // Worker 的 Unix 域套接字名称，即其 RPC 地址
}

// call() 会通过 RPC 将指定的参数 args 发往 srv 服务器上的 rpcName 服务，
// 等待响应，并把响应内容放入到 reply 中。作为响应的参数应为指向响应数据结构的地址。
//
// call() 会在服务器返回响应时返回 true，在无法连接至服务器时返回 false。
// 具体而言，当且仅当 call() 返回 true 时 reply 中的内容才是可用的。
//
// 你应当任务 call() 会在服务器没有及时响应时超时并返回错误。
//
// 请在 master.go、mapreduce.go 和 worker.go 中使用该函数进行 RPC 调用。
// 不要修改该函数。
//
func call(srv string, rpcName string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcName, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
