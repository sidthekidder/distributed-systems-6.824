package mapreduce

import (
	"hash/fnv"
	"io/ioutil"
	"encoding/json"
	"os"
	"log"
)

func doMap(
	jobName string, // the name of the MapReduce job
	mapTask int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(filename string, contents string) []KeyValue,
) {
	// read input from the map task
	contents, err := ioutil.ReadFile(inFile)
	if err != nil {
		log.Fatal(err)
	}

	// pass it to the use-defined mapF function
	output := mapF(inFile, string(contents))

	// init an array of encoders, one for each intermediate file
	encs := make([]*json.Encoder, nReduce)

	for _, i := range output {
		// pick the r (intermediate file number) using ihash of i.key and keep it within nReduce range
		r := ihash(i.Key) % nReduce

		// get the filename
		fileName := reduceName(jobName, mapTask, r)

		// if encoder doesn't exist for this r, create it and save it
		if encs[r] == nil {
			file, err := os.Create(fileName)
			if err != nil {
				log.Fatal(err)
			}

			encs[r] = json.NewEncoder(file)

			// close file later
			defer file.Close()
		}

		// write the json stream to the corresponding file
		encs[r].Encode(i)
	}
}

func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}
