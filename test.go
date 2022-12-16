package main

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"reflect"
	"time"
)

type asd struct {
	a int
	b []byte
	c string
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}
func (a *asd) asd(v1 interface{}, v2 interface{}) {
	//t1 := reflect.TypeOf(v1)
	//t := reflect.ValueOf(v1)
	//fmt.Println(t1.Kind() == t2.Kind())
	//fmt.Println(t1.Kind().String(), t2.Kind().String())
	//asd, _ := t2.FieldByName("a")
	//fmt.Println(t, t.FieldByName("b"))
	aaa := (v1).(asd)
	fmt.Println(aaa)
	//for i := 0; i < t2.NumField(); i++ {
	//	fieldType := t2.Field(i)
	//	fmt.Printf("Tag: %v Type: %v Name:%v \n", fieldType.Tag, fieldType.Type, fieldType.Name)
	//}
}

func main() {
	//db := map[int]int{}
	//db[1] = 11
	//db[2] = 22
	//val, ok := db[1]
	//fmt.Println(val, ok)
	//for a, b := range db {
	//	fmt.Println(a, b)
	//}
	//t := time.Now()
	//for time.Since(t).Seconds() < 1 {
	//	time.Sleep(100 * time.Millisecond)
	//	fmt.Println(1)
	//}
	ch := make(chan int)
	go func() {
		time.Sleep(1500 * time.Millisecond)
		ch <- 1
	}()
	tickTimer := time.NewTicker(1 * time.Second)
	select {
	case c := <-ch:
		fmt.Println(c)
	case <-tickTimer.C:
		fmt.Println("over")
	}
	fmt.Println(11111)
	a := asd{
		a: 1,
	}
	mp := make(map[int]string)
	mp[1] = mp[1]
	fmt.Println(applyMsgToString(&a))
	fmt.Println(a, mp)
	var arr []int
	arr = append(arr, 1)
	arr = append(arr, 1)
	arr = append(arr, 1)
	arr = arr[:0]
	fmt.Println(arr)
	defer func() {
		fmt.Println(111)
	}()
	defer func() {
		fmt.Println(222)
	}()
	defer func() {
		fmt.Println(333)
	}()
	//a := asd{
	//	c: "12",
	//}
	////fmt.Println(a.c == "")
	//a.asd(a, 1)
	//fmt.Println(raft.Raft{})
	//t1 := reflect.TypeOf(a)
	//t2 := reflect.TypeOf("1")
	//fmt.Println(t1.Kind(), t2.Kind())
}
func applyMsgToString(msg *asd) string {
	var str = ""
	t := reflect.TypeOf(*msg)
	v := reflect.ValueOf(*msg)
	for i := 0; i < t.NumField(); i++ {
		fieldType := t.Field(i)
		str += fmt.Sprintf("%v: %v", fieldType.Name, v.FieldByName(fieldType.Name)) + " "
	}
	return str
}
func (a *asd) toString() string {
	return "asd"
}
