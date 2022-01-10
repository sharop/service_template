package badger_test

import (
	badger "github.com/sharop/service_template_dex/distio/dbstore"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"log"
	"os"
	"testing"
)

func TestLog(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T, db *badger.Database,
	){
		"Set key+value succeeds": testSetData,
	} {
		t.Run(scenario, func(t *testing.T) {
			dir, err := ioutil.TempDir("", "badger-test")
			require.NoError(t, err)
			log.Printf("Testing directory : %v", dir)
			defer os.RemoveAll(dir)

			db, err := badger.New(dir)
			require.NoError(t, err)

			fn(t, db)

		})
	}
}

func testSetData(t *testing.T, db *badger.Database) {
	key, value := "KEY_NEW", "SERCH I/O"

	err := db.Set(key, value)
	require.NoError(t, err)
	nonKey := "KEY_ERROR"
	vNonKey := db.Get(nonKey)
	require.Nil(t, vNonKey)
	reqValue := db.Get(key)
	require.EqualValues(t, value, reqValue)
	require.True(t, db.Delete(nonKey))
	require.True(t, db.Delete(key))

}
