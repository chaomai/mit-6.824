package client

import (
	"fmt"
	"testing"
)

func updaateSlice(s *[]int) {
	arr := []int{1, 2, 3}
	*s = append(*s, arr...)
}

func TestPassSlice(t *testing.T) {
	s := make([]int, 0)
	updaateSlice(&s)
	fmt.Println(s)
}
