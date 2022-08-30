package raft

import (
	"fmt"
)

// Log 是 raft 中的日志
type Log struct {
	// 第一个 Log 的日志项的前一个日志项的索引
	Index int
	// 日志项
	// 保证 len 至少为 1
	Log []Entry
}

// Entry 是 Log 中的日志项
type Entry struct {
	Term    int
	Command interface{}
}

func (l *Log) toString() string {
	str := ""
	cnt := 0
	var item *Entry
	for i := maxInt(l.startIndex(), l.lastIndex()-10); i <= l.lastIndex(); i++ {
		item = l.entry(i)
		str += fmt.Sprintf("{index: %v, term: %v, command: %v} ", i, item.Term, item.Command)
		cnt++
		if cnt%3 == 0 {
			str += "\n"
		}
	}
	return str
}

// 产生一个空的 Log
func mkLogEmpty() *Log {
	return &Log{0, make([]Entry, 1)}
}

// 最后一个日志项 index
// 0 for empty
func (l *Log) lastIndex() int {
	return l.Index + len(l.Log) - 1
}

// 最后一个日志项 term
// 0 for empty
func (l *Log) lastTerm() int {
	return l.Log[len(l.Log)-1].Term
}

// 第一个日志项 index
// 0 for empty
func (l *Log) startIndex() int {
	return l.Index
}

// index 的日志项
func (l *Log) entry(index int) *Entry {
	return &l.Log[index-l.Index]
}

// 子区间 [start, end)
func (l *Log) slice(start int, end int) []Entry {
	return l.Log[start-l.Index : end-l.Index]
}

// 新增日志项
func (l *Log) append(entry *Entry) {
	l.Log = append(l.Log, *entry)
}

// 用 entries 替换从 start(included) 开始后面的日志项
func (l *Log) delAndOverlap(start int, entries []Entry) {
	l.Log = l.Log[:start-l.Index]
	l.Log = append(l.Log, entries...)
}

// 剪切掉 index（包括）前所有的日志
func (l *Log) cut(index int) {
	if index <= l.startIndex() || index > l.lastIndex() {
		return
	}
	l.Log = l.slice(index, l.lastIndex()+1)
	l.Index = index
}

// 在 term 下的日志项中，index 最小的日志项的 index
// 传入参数保证存在该日志项
//func (l *Log) oldest(term int) int {
//	for index := l.lastIndex() - 1; index > l.startIndex(); index-- {
//		if l.entry(index).Term != term && l.entry(index + 1).Term == term {
//			return index
//		}
//	}
//	return l.startIndex()
//}
