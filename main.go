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
type ComplexIndex struct {
	start int
	end   int
}
type CityIndex struct {
	start     int
	end       int
	complexes map[string]ComplexIndex
}
type ProvinceIndex struct {
	start  int
	end    int
	cities map[string]CityIndex
}

var gRows []Deal
var IndexProvince map[string]ProvinceIndex
var KoreanDecoder *encoding.Decoder = korean.EUCKR.NewDecoder()

type ResponseTypeUnit struct {
	Name string `json:"title"`
	Key  int    `json:"key"`
}

type ResponseTypeAll struct {
	Result []ResponseTypeUnit `json:"result"`
}

type ResponseTypeComplex struct {
	Result []ResponseComplexTypeUnit `json:"result"`
}
type ResponseComplexTypeUnit struct {
	Name string `json:"title"`
	Key  int    `json:"key"`
	City string `json:"province"`
	Gu   string `json:"gu"`
	Road string `json:"road"`
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

func ImportRows(csvfile string) ([]Deal, map[string]ProvinceIndex, error) {
	rows := make([]Deal, 944410)
	index := make(map[string]ProvinceIndex)

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
	curProvIndexKey := ""    //province
	curCityIndexKey := ""    //city
	curComplexIndexKey := "" //complex
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
		// TODO : fix bug
		if curComplexIndexKey != decode(complex) {
			//log.Printf("complex key different[%s], [%s]", curComplexIndexKey, complex)
			provElement, provOk := index[curProvIndexKey]
			cityElement, cityOk := provElement.cities[curCityIndexKey]
			complexElement, complexOk := cityElement.complexes[curComplexIndexKey]
			if complexOk {
				//log.Printf("complexOk[%s]", curComplexIndexKey)
				complexElement.end = i
				cityElement.complexes[curComplexIndexKey] = complexElement
			}
			curComplexIndexKey = decode(complex)
			newComplexIndex := ComplexIndex{i, -1}

			if curCityIndexKey != decode(city[1]) {
				if cityOk {
					//log.Printf("cityOk[%s]", curCityIndexKey)
					cityElement.end = i
					provElement.cities[curCityIndexKey] = cityElement
				}
				curCityIndexKey = decode(city[1])
				newCityIndex := CityIndex{
					i, -1,
					map[string]ComplexIndex{
						curComplexIndexKey: newComplexIndex},
				}
				if curProvIndexKey != decode(city[0]) {
					if provOk {
						//log.Printf("provOk[%s]", curProvIndexKey)
						provElement.end = i
						index[curProvIndexKey] = provElement
					}
					curProvIndexKey = decode(city[0])
					index[curProvIndexKey] = ProvinceIndex{i, -1,
						map[string]CityIndex{
							curCityIndexKey: newCityIndex},
					}
				} else { // curProvIndexKey is kept on
					provElement.cities[curCityIndexKey] = newCityIndex
				}
			} else { // curCityIndexKey is kept on
				cityElement.complexes[curComplexIndexKey] = newComplexIndex
			}
		}
		i++

	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	return rows, index, nil
}

func HandleComplex(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	index := atoi(vars["city"])
	row := gRows[index]
	city, gu, road := row.City, row.Gu, row.Road
	complexesIndex := IndexProvince[city].cities[gu].complexes
	res := ResponseTypeComplex{make([]ResponseComplexTypeUnit, len(complexesIndex))}
	i := 0
	for complex, v := range complexesIndex {
		res.Result[i] = ResponseComplexTypeUnit{
			complex,
			v.start,
			city, gu, road,
		}
		i++
	}
	b, err := json.Marshal(res)
	if err != nil {
		log.Println(err)
	}
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, "%s\n", b)
}

func HandleCity(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	provinceIndex := atoi(vars["province"])
	province := gRows[provinceIndex].City
	citiesIndex := IndexProvince[province].cities
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
	w.Header().Set("Content-Type", "application/json")
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
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, "%s\n", string(b))
}

func HandleDeals(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := atoi(vars["complex"])
	row := gRows[key]
	indexProvince := IndexProvince[row.City]
	indexCity := indexProvince.cities[row.Gu]
	indexComplex := indexCity.complexes[row.Complex]

	b, err := json.Marshal(gRows[indexComplex.start:indexComplex.end])
	if err != nil {
		log.Panicln(err)
	}
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")
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
	r.HandleFunc("/complexes/{city:[0-9]+}", HandleComplex)
	r.HandleFunc("/deals/{complex:[0-9]+}", HandleDeals)
	r.HandleFunc("/deal/{id:[0-9]+}/$", HandlePrice)
	http.Handle("/", r)
	log.Fatal(http.ListenAndServe(":8080", nil))
}
