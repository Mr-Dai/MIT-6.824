package mapreduce

import (
	"os"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

type StringSlice []string

func (s StringSlice) Len() int {
	return len(s)
}

func (s StringSlice) Less(i, j int) bool {
	return strings.Compare(s[i], s[j]) == -1
}

func (s StringSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// doReduce 负责一个 Reduce 任务：它会读入由 Map 阶段产生的中间结果键值对，
// 依键的顺序对键值对进行排序，调用用户指定的 Reduce 函数，并把输出写出到磁盘上
func doReduce(
	jobName string, // MapReduce 作业的名称
	reduceTaskNumber int, // 当前是哪个 Reduce 任务
	outFile string, // 输出文件路径
	nMap int, // Map 任务的数量
	reduceF func(key string, values []string) string,
) {
	// 你需要编写这个函数
	//
	// 你需要从每个 Map 任务读取一个中间文件，reduceName(jobName, m, reduceTaskNumber)
	// 会返回 Map 任务 m 中你需要读取的文件。
	//
	// 你的 doMap 函数会把键值对编码至中间文件中，因此你需要对它们进行解码。
	// 如果你使用了 JSON 格式，你可以读取文件的内容，再通过创建一个解码器
	// 并不断地对其调用 .Decode(&kv) 进行解码，直到它返回一个错误
	//
	// Go 语言 sort 包文档的第一个例子可能会对你有所帮助。
	//
	// reduceF() 是应用提供的 Reduce 函数。对于每一个不同的键你都需要调用它，
	// 并提供该键对应所有值组成的切片。reduceF() 会返回该键归约后的值。
	//
	// 你需要把 Reduce 输出的 KeyValue 以 JSON 格式写入到名为 outFile 的
	// 文件中。我们要求你使用 JSON 是因为后面负责对输出进行合并的 Merger
	// 期望这些输出是 JSON 格式的。JSON 格式本身没什么特别的，只是一种我们选
	// 择使用的编码格式罢了。你的输出代码看起来可能会像下面这样：
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//

	// !!! 以下是 Mr-Dai 的参考实现 !!!

	// 读取中间结果文件
	inFiles := make([]*os.File, nMap)
	decs := make([]*json.Decoder, nMap)
	for i := 0; i < nMap; i++ {
		inFileName := reduceName(jobName, i, reduceTaskNumber)
		inFile, err := os.OpenFile(inFileName, os.O_RDONLY, 0600)
		if err != nil {
			fmt.Printf("Failed to open REDUCE input file %s: %v\n", inFileName, err)
			return
		}
		defer inFile.Close()
		inFiles[i] = inFile
		decs[i] = json.NewDecoder(inFile)
	}

	inKVs := make(map[string][]string)
	for i := 0; i < nMap; i++ {
		for {
			var kv KeyValue
			err := decs[i].Decode(&kv)
			if err != nil {
				if err.Error() != "EOF" {
					fmt.Printf("Failed to read from REDUCE input file %s: %v\n", inFiles[i].Name(), err)
				}
				break
			}
			inKVs[kv.Key] = append(inKVs[kv.Key], kv.Value)
		}
	}

	// 对 Key 进行排序
	keys := make([]string, len(inKVs))
	var i = 0
	for k := range inKVs {
		keys[i] = k
		i++
	}
	sort.Sort(StringSlice(keys))

	// 创建输出文件
	out, err := os.OpenFile(outFile, os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		fmt.Printf("Failed to create REDUCE output file %s: %vn", outFile, err)
		return
	}
	defer out.Close()
	enc := json.NewEncoder(out)

	// 运行用户 Reduce 函数，写出结果
	for _, key := range keys {
		values := inKVs[key]
		reduced := reduceF(key, values)
		enc.Encode(KeyValue{key, reduced})
	}
}
