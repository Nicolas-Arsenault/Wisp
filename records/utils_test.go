// Writing records to disk, etc...
package records

import (
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
