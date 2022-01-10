package main

import (
	"fmt"
	"github.com/dgraph-io/badger/v3"
	"log"
)

// CReate log with badger
// Add to dristribute log
// Integrate with cluster project
func main() {
	db, err := badger.Open(badger.DefaultOptions("./badger_log"))
	if err != nil {
		log.Fatalln(err)
	}
	defer db.Close()

	err = db.Update(func(txn *badger.Txn) error {
		k := "TEMPO"
		v := "Algunos datos"
		err := txn.Set([]byte(k), []byte(v))
		if err != nil {
			log.Fatalln(
				err,
			)
		}
		return nil
	})
	if err != nil {
		log.Fatalln(err)
	}
	var valCopy []byte
	err = db.View(func(txn *badger.Txn) error {
		itm, err := txn.Get([]byte("TEMPO"))
		if err != nil {
			return err
		}
		log.Printf("Value Item %v \n", itm.Value(func(val []byte) error {
			fmt.Printf("Value %s \n", val)
			valCopy = append([]byte{}, val...)
			return nil
		}),
		)

		return nil
	})

	fmt.Printf("Value copy %s \n", valCopy)

}
