package mapreduce

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"os"
)

// doMap 负责一个 Map 任务：它会读入一个输入文件（inFile），调用用户给定的 Map
// 函数（mapF），并将输出分片至 nReduce 个中间文件中
func doMap(
	jobName string, // MapReduce 作业的名称
	mapTaskNumber int, // 当前是哪个 Map 任务
	inFile string,
	nReduce int, // Reduce 任务的数量
	mapF func(file string, contents string) []KeyValue,
) {
	// 你需要编写这个函数。
	//
	// 一个 Map 任务的中间输出需要被存储在不同的文件中，每个 Reduce 任务
	// 对应一个。文件名应包含当前 Map 任务索引值和 Reduce 任务索引值。
	// 可以将 reduceName(jobName, mapTaskNumber, r) 生成的文件名用于
	// Reduce 任务 r 的中间文件。对于每个键值对，用键调用 ihash()（见下方）、
	// 对 nReduce 取模，并将结果作为对应的 r。
	//
	// mapF() 是由应用提供的 Map 函数。第一个变量应是输入文件的名称，尽管
	// Map 函数多半都会忽略该输入。第二个参数应是整个输入文件的内容。
	// mapF 会返回一个由用于 Reduce 的任务的键值对组成的切片。
	// 请在 common.go 中查阅 KeyValue 的定义。
	//
	// 请在 Go 的 ioutil 和 os 包中查找读写文件用的函数。
	//
	// 要设计出一个在磁盘上组织键值对的方式是不容易的，尤其是还要考虑键与值都可
	// 能包含换行符、引号和其他任何你可能想到的字符。
	//
	// JSON 就是常被用于将数据序列化为对端可轻易重新构建的字节流的一种格式，
	// 但考虑到 Reduce 任务的输出 *必须* 是 JSON，现在就了解一下它可能会对你有所
	// 帮助。你可以使用下面这段被注释的代码将一个数据结构写出为 JSON 字符串。
	// 在 common_reduce.go 中可以找到对应的解码函数。
	//
	//   enc := json.NewEncoder(file)
	//   for _, kv := ... {
	//     err := enc.Encode(&kv)
	//
	// 记得在你写出所有的值以后关闭文件！
	//

	// !!! 以下是 Mr-Dai 的参考实现 !!!

	// 读取输入
	fileBytes, err := ioutil.ReadFile(inFile)
	if err != nil {
		fmt.Printf("Failed to open MAP input file %s: %v\n", inFile, err)
		return
	}

	// 创建输出文件
	outFiles := make([]*os.File, nReduce)
	encs := make([]*json.Encoder, nReduce)
	for i := 0; i < nReduce; i++ {
		outFileName := reduceName(jobName, mapTaskNumber, i)
		outFile, err := os.OpenFile(outFileName, os.O_CREATE|os.O_WRONLY, 0600)
		if err != nil {
			fmt.Printf("Failed to create MAP output file %s: %v\n", outFileName, err)
			return
		}
		defer outFile.Close()
		outFiles[i] = outFile
		encs[i] = json.NewEncoder(outFiles[i])
	}

	// 运行用户 Map 函数，写出结果
	kvs := mapF(inFile, string(fileBytes))
	for _, kv := range kvs {
		r := ihash(kv.Key) % nReduce
		encs[r].Encode(&kv)
	}
}

func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}
