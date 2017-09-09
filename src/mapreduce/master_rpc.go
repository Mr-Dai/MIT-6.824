package mapreduce

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
)

// Shutdown 是一个可以关闭 Master RPC 服务器的 RPC 方法
func (mr *Master) Shutdown(_, _ *struct{}) error {
	debug("Shutdown: registration server\n")
	close(mr.shutdown)
	mr.l.Close() // 令其 Accept 方法失效
	return nil
}

// startRPCServer 会启动 Master 的 RPC 服务器。只要 Worker 仍然存活，
// 它就会一直接受 RPC 调用
func (mr *Master) startRPCServer() {
	s := rpc.NewServer()
	s.Register(mr)
	os.Remove(mr.address) // 只在 Unix 上有必要
	l, e := net.Listen("unix", mr.address)
	if e != nil {
		log.Fatal("RegstrationServer", mr.address, " error: ", e)
	}
	mr.l = l

	// 既然我们已经在监听 Master 地址了，我们就可以在另一个线程中接受连接了
	go func() {
	loop:
		for {
			select {
			case <-mr.shutdown:
				break loop
			default:
			}
			conn, err := mr.l.Accept()
			if err == nil {
				go func() {
					s.ServeConn(conn)
					conn.Close()
				}()
			} else {
				debug("RegistrationServer: accept error", err)
				break
			}
		}
		debug("RegistrationServer: done\n")
	}()
}

// stopRPCServer 会关闭 Master 的 RPC 服务器
// 这个操作必须通过 RPC 调用完成，以避免 RPC 服务器线程与当前线程间的竞态
func (mr *Master) stopRPCServer() {
	var reply ShutdownReply
	ok := call(mr.address, "Master.Shutdown", new(struct{}), &reply)
	if ok == false {
		fmt.Printf("Cleanup: RPC %s error\n", mr.address)
	}
	debug("cleanupRegistration: done\n")
}
