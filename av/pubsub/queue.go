// Packege pubsub implements publisher-subscribers model used in multi-channel streaming.
package pubsub

import (
	"github.com/nareix/joy4/av"
	"github.com/nareix/joy4/av/pktque"
	"io"
	"sync"
	"time"
)

//        time
// ----------------->
//
// V-A-V-V-A-V-V-A-V-V
// |                 |
// 0        5        10
// head             tail
// oldest          latest
//

// One publisher and multiple subscribers thread-safe packet buffer queue.
type Queue struct {
	buf                      *pktque.Buf
	head, tail               int
	lock                     *sync.RWMutex
	cond                     *sync.Cond
	curgopcount, maxgopcount int
	streams                  []av.CodecData
	videoidx                 int
	closed                   bool
}

func NewQueue() *Queue {
	q := &Queue{}
	q.buf = pktque.NewBuf()
	q.maxgopcount = 2
	q.lock = &sync.RWMutex{}
	q.cond = sync.NewCond(q.lock.RLocker())
	q.videoidx = -1
	return q
}

// 缓存 GOP 数目
func (self *Queue) SetMaxGopCount(n int) {
	self.lock.Lock()
	self.maxgopcount = n
	self.lock.Unlock()
	return
}

// 设置 rtmp 流元数据
func (self *Queue) WriteHeader(streams []av.CodecData) error {
	self.lock.Lock()
	self.streams = streams
	for i, stream := range streams {
		// 设置 视频流索引 videoidx 
		if stream.Type().IsVideo() {
			self.videoidx = i
		}
	}
	self.cond.Broadcast()
	self.lock.Unlock()
	return nil
}

func (self *Queue) WriteTrailer() error {
	return nil
}

// After Close() called, all QueueCursor's ReadPacket will return io.EOF.
func (self *Queue) Close() (err error) {
	self.lock.Lock()

	self.closed = true
	self.cond.Broadcast()

	self.lock.Unlock()
	return
}

// Put packet into buffer, old packets will be discared.
func (self *Queue) WritePacket(pkt av.Packet) (err error) {
	self.lock.Lock()

	// pkt 入队
	self.buf.Push(pkt)

	// 若是视频帧、且是关键帧，则更新 GOP 计数
	if pkt.Idx == int8(self.videoidx) && pkt.IsKeyFrame {
		self.curgopcount++
	}

	// 如果当前缓存的 GOP 总数超过阈值，就 Pop 出冗余的视频帧
	for self.curgopcount >= self.maxgopcount && self.buf.Count > 1 {
		pkt := self.buf.Pop()
		if pkt.Idx == int8(self.videoidx) && pkt.IsKeyFrame {
			self.curgopcount--
		}
		if self.curgopcount < self.maxgopcount {
			break
		}
	}
	//println("shrink", self.curgopcount, self.maxgopcount, self.buf.Head, self.buf.Tail, "count", self.buf.Count, "size", self.buf.Size)

	// 信号量广播，唤醒所有阻塞在 ReadPacket() 函数中的协程
	self.cond.Broadcast()

	self.lock.Unlock()
	return
}

type QueueCursor struct {
	que    *Queue        // 指向底层 Queue 队列 
	pos    pktque.BufPos // 读取 Queue 的位置下标
	gotpos bool          // 默认值false，意味着 pos 尚未初始化
	init   func(buf *pktque.Buf, videoidx int) pktque.BufPos // 初始化函数，用于设置 pos 初值
}


// 
func (self *Queue) newCursor() *QueueCursor {
	return &QueueCursor{
		que: self,
	}
}


// Create cursor position at latest packet.
func (self *Queue) Latest() *QueueCursor {
	cursor := self.newCursor()
	cursor.init = func(buf *pktque.Buf, videoidx int) pktque.BufPos {
		return buf.Tail
	}

	return cursor
}

// Create cursor position at oldest buffered packet.
func (self *Queue) Oldest() *QueueCursor {
	cursor := self.newCursor()
	cursor.init = func(buf *pktque.Buf, videoidx int) pktque.BufPos {
		return buf.Head
	}
	return cursor
}

// Create cursor position at specific time in buffered packets.
func (self *Queue) DelayedTime(dur time.Duration) *QueueCursor {
	cursor := self.newCursor()
	cursor.init = func(buf *pktque.Buf, videoidx int) pktque.BufPos {
		i := buf.Tail - 1
		if buf.IsValidPos(i) {
			end := buf.Get(i)
			for buf.IsValidPos(i) {
				if end.Time-buf.Get(i).Time > dur {
					break
				}
				i--
			}
		}
		return i
	}
	return cursor
}

// Create cursor position at specific delayed GOP count in buffered packets.
func (self *Queue) DelayedGopCount(n int) *QueueCursor {
	cursor := self.newCursor()
	cursor.init = func(buf *pktque.Buf, videoidx int) pktque.BufPos {
		i := buf.Tail - 1
		if videoidx != -1 {
			for gop := 0; buf.IsValidPos(i) && gop < n; i-- {
				pkt := buf.Get(i)
				if pkt.Idx == int8(self.videoidx) && pkt.IsKeyFrame {
					gop++
				}
			}
		}
		return i
	}
	return cursor
}



func (self *QueueCursor) Streams() (streams []av.CodecData, err error) {
	self.que.cond.L.Lock()

	// 这个 for + wait 的作用是等待 self.que.streams 被赋值或者 self.que 被关闭
	for self.que.streams == nil && !self.que.closed {
		self.que.cond.Wait()
	}

	// for + wait 退出后: 
	// 如果 self.que.streams 被赋值就取出并返回；
	// 如果 self.que 被关闭就直接返回 err 。
	if self.que.streams != nil {
		streams = self.que.streams //设置返回值
	} else {
		err = io.EOF
	}

	self.que.cond.L.Unlock()
	return
}

// ReadPacket will not consume packets in Queue, it's just a cursor.
func (self *QueueCursor) ReadPacket() (pkt av.Packet, err error) {
	self.que.cond.L.Lock()
	
	// 指向底层 pkg 队列
	buf := self.que.buf // *pktque.Buf

	// 若 self.gotpos 值为 false（默认值），此时需要初始化 self.pos
	if !self.gotpos {
		// 设置 pos 的值
		self.pos = self.init(buf, self.que.videoidx)
		// 避免重复初始化
		self.gotpos = true
	}

	for {

		// 将读取指针 pos 修正到 [ buf.Head, ... , buf.Tail ] 之间
		if self.pos.LT(buf.Head) {
			self.pos = buf.Head
		} else if self.pos.GT(buf.Tail) {
			self.pos = buf.Tail
		}

		// 检查是否满足 buf.Head <= self.pos < buf.Tail 
		if buf.IsValidPos(self.pos) {
			// 取出 self.pos 位置元素 pkt
			pkt = buf.Get(self.pos)
			// 更新读取下标
			self.pos++
			// 退出循环，返回 pkg 
			break
		}

		// 若经过修正之后有 self.pos = buf.Tail ，则会走到这里，此时意味着队列为空，
		// 检查一下 self.que 是否被关闭，如果被关闭就直接返回 err
		if self.que.closed {
			err = io.EOF
			break
		}

		// 至此，意味着 self.que 未关闭，但是当前没有新的可读数据，进入 wait 阻塞等待
		self.que.cond.Wait()
	}


	self.que.cond.L.Unlock()
	return
}
