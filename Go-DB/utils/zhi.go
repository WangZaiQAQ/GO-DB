package utils

import "fmt"

func main() {
	a := make([]int, 0)
	a = append(a, 2)
	fmt.Printf("main value is", a)
	fmt.Printf("a adderss is", &a)
	copyList(a)
}

func copyList(a []int) {
	a = append(a, 3)
	fmt.Printf("main value is", a)
	fmt.Printf("a adderss is", &a)
}
