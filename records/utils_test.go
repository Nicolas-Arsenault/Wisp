// Writing records to disk, etc...
package records

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestAppendToFile(t *testing.T) {
	appendToFile("./test.bin", "Hey, its me Nick!")
	err, data := readFromFile("./test.bin")
	if err != nil {
		t.Error("Threw an error when reading from file.")
	}
	if "Hey, its me Nick!" != string(data) {
		t.Error("Error when reading from file, input data altered.")
	}
	fmt.Println(string(data))
}

func TestDecoratePayload(t *testing.T) {
	s, err := DecoratePayload("{\"test\": 123}")
	if err != nil {
		t.Error("Error while decorating the payload." + err.Error())
	}
	var data map[string]interface{}
	json.Unmarshal([]byte(s), &data)
	if data["seq_n"] == nil {
		t.Error("Sequence number field not present on decorated data.")
	}
	DecoratePayload("{\"test\": 321}")
	if seqNum != 2 {
		t.Error("Sequence number didnt increment correctly")
	}

}
