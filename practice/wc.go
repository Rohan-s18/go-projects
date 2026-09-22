package main

import (
	"container/list"
	"fmt"
	"os"
	"strconv"
	"strings"
	"unicode"

	"coms4113/hw1/src/mapreduce"
)

func Map(value string) *list.List {
	result := list.New()

	words := strings.FieldsFunc(value, func(r rune) bool {
		return !unicode.IsLetter(r)
	})

	for _, word := range words {
		result.PushBack(mapreduce.KeyValue{
			Key:   word,
			Value: "1",
		})
	}

	return result
}

func Reduce(key string, values *list.List) string {
	total := 0

	for element := values.Front(); element != nil; element = element.Next() {
		value := element.Value.(string)

		count, err := strconv.Atoi(value)
		if err != nil {
			panic(err)
		}

		total += count
	}

	return strconv.Itoa(total)
}
