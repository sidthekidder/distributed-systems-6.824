package mapreduce

import (
	"os"
	"io"
	"log"
	"encoding/json"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	// empty array for collecting values
	valuesList := make(map[string][]string)

	for m := 0 ; m < nMap ; m++ {
		// get the intermediate filename
		fileName := reduceName(jobName, m, reduceTask)

		// decode the file
		tempFile, err := os.Open(fileName)
		if err != nil {
			log.Fatal(err)
		}

		dec := json.NewDecoder(tempFile)

		// read all the values and append to list
		for {
			var val KeyValue
			err := dec.Decode(&val)

			if err == io.EOF {
				break
			} else if err != nil {
				log.Fatal(err)
				break
			}

			// append the value read to the corresponding key
			valuesList[val.Key] = append(valuesList[val.Key], val.Value)
		}
	}

	// create the final file required
	finalFile, err := os.Create(outFile)
	if err != nil {
		log.Fatal(err)
	}
	defer finalFile.Close()

	// connect the encoder with the final output file
	enc := json.NewEncoder(finalFile)

	// loop through each intermediate key and write the json stream to the output file
	for k, v := range valuesList {
		enc.Encode(KeyValue{k, reduceF(k, v)})
	}
}
