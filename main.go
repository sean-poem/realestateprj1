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

	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/korean"

	"github.com/gorilla/mux"
)

type Deal struct {
	Idx           int
	City          string
	Gu            string
	Dong          string
	Complex       string
	AreaExclusive string
	Year          int
	Month         int
	Date          int
	Price         int
	Floor         int
	YearBuilt     int
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
var KoreanDecoder *encoding.Decoder = korean.EUCKR.NewDecoder()

type ResponseTypeUnit struct {
	Name string `json:"title"`
	Key  int    `json:"key"`
}

type ResponseTypeAll struct {
	Result []ResponseTypeUnit `json:"result"`
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

func decode(name string) string {
	decoded, err := KoreanDecoder.String(name)
	if err != nil {
		log.Fatal(err)
	}
	return decoded
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
	var year, month int
	var date, price, floor, yearBuilt int

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
		year = atoi(unquote(columns[6])[:4])
		month = atoi(unquote(columns[6])[4:])
		date = atoi(columns[7])
		price = atoi(strings.ReplaceAll(columns[8], ",", ""))
		floor = atoi(columns[9])
		yearBuilt = atoi(columns[10])
		road = unquote(columns[11])

		rows[i] = Deal{
			Idx:           i,
			City:          decode(city[0]),
			Gu:            decode(city[1]),
			Complex:       decode(complex),
			AreaExclusive: areaExclusive,
			Year:          year,
			Month:         month,
			Date:          date,
			Price:         price,
			Floor:         floor,
			YearBuilt:     yearBuilt,
			Road:          decode(road),
		}
		if curIndex != decode(city[0]) {
			if v, ok := index[curIndex]; ok {
				index[curIndex] = Index{v.start, i, v.childs}
			}
			//log.Printf("curIndex: %s", curIndex)
			curIndex = decode(city[0])
			cindex := make(map[string]ChildIndex)
			index[curIndex] = Index{i, -1, cindex}
			curChildIndex = ""
		}
		if curChildIndex != decode(city[1]) {
			parent := index[curIndex]
			if v, ok := parent.childs[curChildIndex]; ok {
				parent.childs[curChildIndex] = ChildIndex{v.start, i}
			}
			curChildIndex = decode(city[1])
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

		res.Result[i] = ResponseTypeUnit{city, v.start}
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
		res.Result[i].Name = prov
		res.Result[i].Key = v.start
		i++
	}
	//log.Printf("%v", res.results)
	b, err := json.Marshal(res)
	if err != nil {
		log.Println(err)
	}
	log.Printf("%s", string(b))
	w.Header().Set("Access-Control-Allow-Origin", "*")
	fmt.Fprintf(w, "%s\n", string(b))
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
	KoreanDecoder = korean.EUCKR.NewDecoder()
	r := mux.NewRouter()
	r.HandleFunc("/provinces", HandleProvince)
	r.HandleFunc("/cities/{province:[0-9]+}", HandleCity)
	r.HandleFunc("/deals/{city:[0-9]+}", HandleDeals)
	r.HandleFunc("/deal/{id:[0-9]+}/$", HandlePrice)
	http.Handle("/", r)
	log.Fatal(http.ListenAndServe(":8080", nil))
}
