package pktque

import (
	"github.com/nareix/joy4/av"
)

type Buf struct {
	Head, Tail BufPos
	pkts       []av.Packet
	Size       int
	Count      int
}

func NewBuf() *Buf {
	return &Buf{
		pkts: make([]av.Packet, 64),
	}
}

// 取出头部元素
func (self *Buf) Pop() av.Packet {
	if self.Count == 0 {
		panic("pktque.Buf: Pop() when count == 0")
	}
	// i 为 self.Head 对应的元素下标
	i := int(self.Head) & (len(self.pkts) - 1)
	// 取出 self.Head 对应的消息 pkt
	pkt := self.pkts[i]
	// 重置 self.Head 消息（让旧的 Packet 被 GC 清理掉，以释放资源）
	self.pkts[i] = av.Packet{}
	// 总字节总数目更新
	self.Size -= len(pkt.Data)
	// 下标后移
	self.Head++
	// 数目减 1
	self.Count--
	// 返回 pkt
	return pkt
}

// 扩容，相当于 reallocate
func (self *Buf) grow() {
	// 扩容
	newpkts := make([]av.Packet, len(self.pkts)*2)
	// 复制
	for i := self.Head; i.LT(self.Tail); i++ {
		newpkts[int(i)&(len(newpkts)-1)] = self.pkts[int(i)&(len(self.pkts)-1)]
	}
	// 重新赋值
	self.pkts = newpkts
}

// 插入元素到尾部
func (self *Buf) Push(pkt av.Packet) {

	// 如果 len(self.pkts) 容量已经没有空余，需要扩容，这里调用 grow 扩大一倍，没有上限，可能有内存问题。
	if self.Count == len(self.pkts) {
		self.grow()
	}

	// 将新 pkt 放到队列 tail 位置
	self.pkts[int(self.Tail)&(len(self.pkts)-1)] = pkt

	// 更新指针、计数、字节总数
	self.Tail++
	self.Count++
	self.Size += len(pkt.Data)

}

// 取出指定位置元素
func (self *Buf) Get(pos BufPos) av.Packet {
	// pkts[ pos % length ]
	return self.pkts[int(pos)&(len(self.pkts)-1)]
}

func (self *Buf) IsValidPos(pos BufPos) bool {
	// pos => [self.Head, self.Tail)
	return pos.GE(self.Head) && pos.LT(self.Tail)
}

type BufPos int

func (self BufPos) LT(pos BufPos) bool {
	return self-pos < 0
}

func (self BufPos) GE(pos BufPos) bool {
	return self-pos >= 0
}

func (self BufPos) GT(pos BufPos) bool {
	return self-pos > 0
}
