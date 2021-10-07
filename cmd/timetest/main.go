package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	gonanoid "github.com/matoous/go-nanoid"
)

func main() {
	v := time.Now().UnixMilli()
	fmt.Println(v)

	s10 := strconv.FormatInt(v, 10)
	fmt.Printf("%T, %v\n", s10, s10)

	s32 := strings.ToUpper(strconv.FormatInt(v, 36))
	fmt.Printf("%T, %v\n", s32, s32)

	////
	nowInMs := time.Now().UnixMilli()
	timeId := strings.ToUpper(strconv.FormatInt(nowInMs, 36))
	shortid := gonanoid.MustGenerate("1234567890ABCDEF", 3)
	finalId := timeId + "-" + shortid
	fmt.Println(finalId)

}
