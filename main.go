package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
)

type Deal struct {
	Idx           int
	City          string
	Gu            string
	Dong          string
	Complex       string
	AreaExclusive string
	Year          uint8
	Month         uint8
	Date          uint8
	Price         uint16
	Floor         int8
	YearBuilt     uint8
	Road          string
}
type ChildIndex struct {
	start int
	end   int
}
type Index struct {
	start  int
	end    int
	childs map[string]ChildIndex
}

var gRows []Deal
var IndexProvince map[string]Index

type ResponseTypeUnit struct {
	name string
	key  int
}

type ResponseTypeAll struct {
	results []ResponseTypeUnit
}

func unquote(val string) string {
	str := strings.Trim(val, "\"")

	return str
}
func atoi(val string) int {
	strval := unquote(val)
	intval, err := strconv.Atoi(strval)
	if err != nil {
		log.Fatal(err)
	}
	return intval
}

func ImportRows(csvfile string) ([]Deal, map[string]Index, error) {
	rows := make([]Deal, 944410)
	index := make(map[string]Index)

	f, err := os.Open(csvfile)
	if nil != err {
		log.Fatal(err)
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	var city []string
	var complex, areaExclusive, road string
	var year, month, date, price, floor, yearBuilt int

	i := 0

	for ; i < 16; i++ {
		scanner.Scan()
	}

	i = 0
	curIndex := ""      //province
	curChildIndex := "" //city
	for scanner.Scan() {
		scanned := scanner.Text()
		columns := strings.Split(scanned, "\",")

		city = strings.Split(unquote(columns[0]), " ")
		complex = unquote(columns[4])
		areaExclusive = unquote(columns[5])
		year = atoi(columns[6])
		month = atoi(columns[6])
		date = atoi(columns[7])
		price = atoi(strings.ReplaceAll(columns[8], ",", ""))
		floor = atoi(columns[9])
		yearBuilt = atoi(columns[10])
		road = unquote(columns[11])

		rows[i] = Deal{
			Idx:           i,
			City:          city[0],
			Gu:            city[1],
			Complex:       complex,
			AreaExclusive: areaExclusive,
			Year:          uint8(year),
			Month:         uint8(month),
			Date:          uint8(date),
			Price:         uint16(price),
			Floor:         int8(floor),
			YearBuilt:     uint8(yearBuilt),
			Road:          road,
		}
		if curIndex != city[0] {
			if v, ok := index[curIndex]; ok {
				index[curIndex] = Index{v.start, i, v.childs}
			}
			//log.Printf("curIndex: %s", curIndex)
			curIndex = city[0]
			cindex := make(map[string]ChildIndex)
			index[curIndex] = Index{i, -1, cindex}
			curChildIndex = ""
		}
		if curChildIndex != city[1] {
			parent := index[curIndex]
			if v, ok := parent.childs[curChildIndex]; ok {
				parent.childs[curChildIndex] = ChildIndex{v.start, i}
			}
			curChildIndex = city[1]
			parent.childs[curChildIndex] = ChildIndex{i, -1}
		}
		i++

	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	return rows, index, nil
}

func HandleCity(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	provinceIndex := atoi(vars["province"])
	province := gRows[provinceIndex].City
	citiesIndex := IndexProvince[province].childs
	res := ResponseTypeAll{make([]ResponseTypeUnit, len(citiesIndex))}
	i := 0
	for city, v := range citiesIndex {
		res.results[i] = ResponseTypeUnit{city, v.start}
		i++
	}
	b, err := json.Marshal(res)
	if err != nil {
		log.Println(err)
	}
	w.Header().Set("Access-Control-Allow-Origin", "*")
	fmt.Fprintf(w, "%s\n", b)
}

func HandleProvince(w http.ResponseWriter, r *http.Request) {
	//fmt.Fprintf(w, "provinces, %d", len(IndexProvince))
	res := ResponseTypeAll{make([]ResponseTypeUnit, len(IndexProvince))}
	i := 0
	for prov, v := range IndexProvince {
		res.results[i] = ResponseTypeUnit{prov, v.start}
		i++
	}
	log.Printf("%s", res.results)
	b, err := json.Marshal(res)
	if err != nil {
		log.Println(err)
	}
	log.Printf("%s", b)
	w.Header().Set("Access-Control-Allow-Origin", "*")
	fmt.Fprintf(w, "%s\n", b)
}

func HandleDeals(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	city := atoi(vars["city"])
	row := gRows[city]
	indexProvince := IndexProvince[row.City]
	indexCity := indexProvince.childs[row.Gu]

	b, err := json.Marshal(gRows[indexCity.start:indexCity.end])
	if err != nil {
		log.Panicln(err)
	}
	w.Header().Set("Access-Control-Allow-Origin", "*")
	fmt.Fprintf(w, "%s", b)
}

func HandlePrice(w http.ResponseWriter, r *http.Request) {
	for i := 0; i < 10; i++ {
		fmt.Fprintf(w, "%s\n", gRows[i])
	}
	fmt.Fprintf(w, "test")
}

func main() {
	if len(os.Args) < 2 {
		log.Fatalln("A data file path should be passed.")
	}
	csvFile := os.Args[1]
	gRows, IndexProvince, _ = ImportRows(csvFile)
	r := mux.NewRouter()
	r.HandleFunc("/provinces", HandleProvince)
	r.HandleFunc("/cities/{province:[0-9]+}", HandleCity)
	r.HandleFunc("/deals/{city:[0-9]+}", HandleDeals)
	r.HandleFunc("/deal/{id:[0-9]+}/$", HandlePrice)
	http.Handle("/", r)
	log.Fatal(http.ListenAndServe(":8080", nil))
}
