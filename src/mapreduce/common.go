package mapreduce

import (
	"fmt"
	"strconv"
)

// 是否开启 Debug 模式
const debugEnabled = false

// debug() 只在 debugEnabled = true 时才会打印
func debug(format string, a ...interface{}) (n int, err error) {
	if debugEnabled {
		n, err = fmt.Printf(format, a...)
	}
	return
}

// jobPhase 用于标识一个任务是作为 Map 任务还是 Reduce 任务被调度
type jobPhase string

const (
	mapPhase    jobPhase = "Map"
	reducePhase          = "Reduce"
)

// KeyValue 是用作将键值对传入到 Map 和 Reduce 函数的数据结构
type KeyValue struct {
	Key   string
	Value string
}

// reduceName 可构建指定 Map 任务生成给指定 Reduce 任务的中间文件的文件名
func reduceName(jobName string, mapTask int, reduceTask int) string {
	return "mrtmp." + jobName + "-" + strconv.Itoa(mapTask) + "-" + strconv.Itoa(reduceTask)
}

// mergeName 可构建指定 Reduce 任务输出文件的文件名
func mergeName(jobName string, reduceTask int) string {
	return "mrtmp." + jobName + "-res-" + strconv.Itoa(reduceTask)
}
