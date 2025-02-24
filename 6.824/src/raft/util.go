package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

// 获取l~r毫秒范围内一个随机毫秒数
func getRandMS(l int, r int) int {
	// 如果每次调rand.Intn()前都调了rand.Seed(x)，每次的x相同的话，每次的rand.Intn()也是一样的（伪随机）
	// 推荐做法：只调一次rand.Seed()：在全局初始化调用一次seed，每次调rand.Intn()前都不再调rand.Seed()。
	// 此处采用使Seed中的x每次都不同来生成不同的随机数，x采用当前的时间戳
	rand.Seed(time.Now().UnixNano())
	ms := l + (rand.Intn(r - l)) // 生成l~r之间的随机数（毫秒）
	return ms
}
