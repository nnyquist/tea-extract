package main

import (
	"database/sql"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
	"gopkg.in/yaml.v3"
)

const maxConcurrent int = 10

type config struct {
	Delimiter string   `yaml:"delimiter"`
	Server    string   `yaml:"server"`
	Database  string   `yaml:"database"`
	Queries   []string `yaml:"queries"`
	OutFiles  []string `yaml:"outfiles"`
}

func main() {
	// read in parameters
	configFile := flag.String("config", "config.yaml", "A YAML file with list of configurations for SQL Extraction.")
	flag.Parse()
	data, err := os.ReadFile(*configFile)
	if err != nil {
		log.Fatal(err)
	}

	params := config{}
	err = yaml.Unmarshal(data, &params)
	if err != nil {
		log.Fatal(err)
	}

	// start timer
	stop := startTimer(&params)
	defer stop()

	// process requests
	waitChan := make(chan struct{}, maxConcurrent)
	wg := sync.WaitGroup{}

	db, err := sqlConnect(&params)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	wg.Add(len(params.Queries))
	delim := []rune(params.Delimiter)[0]

	for i, query := range params.Queries {
		waitChan <- struct{}{}
		outFile := params.OutFiles[i]
		go func(query, outFile string) {
			defer wg.Done()
			defer log.Printf("Extraction completed for %s\n", outFile)
			err := exportData(db, query, outFile, delim)
			if err != nil {
				log.Fatal(err)
			}
			<-waitChan
		}(query, outFile)
	}

	wg.Wait()

}

// startTimer returns a function to defer that will calculate total run time.
func startTimer(c *config) func() {
	t := time.Now()
	log.Printf("Begin extraction process for %s on %s.\n", c.Database, c.Server)
	return func() {
		d := time.Now().Sub(t)
		log.Println("Completed extraction process in", d)
	}
}

// sqlConnect uses the provided configuration to connect to SQL and return the *sql.DB
func sqlConnect(c *config) (*sql.DB, error) {
	connectionString := fmt.Sprintf("server=%s;user_id=;database=%s;", c.Server, c.Database)
	db, err := sql.Open("sqlserver", connectionString)
	if err != nil {
		return nil, fmt.Errorf("Could not connect to SQL Server: %v\n", err)
	}

	return db, nil
}

// exportData queries data from the SQL connection and saves it to the network.
func exportData(db *sql.DB, query, outFile string, delimiter rune) error {
	// create file for export
	csvFile, err := os.Create(outFile)
	if err != nil {
		return fmt.Errorf("Could not create file %s: %v\n", outFile, err)
	}
	defer csvFile.Close()

	// prepare csv writer
	w := csv.NewWriter(csvFile)
	w.Comma = delimiter
	defer w.Flush()

	// query the database
	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("Unable to execute the provided query '%s': %v\n", query, err)
	}
	defer rows.Close()

	// write the column names to csv
	cols, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("Columns could not be collected from the query result: %v\n", err)
	}
	if err := w.Write(cols); err != nil {
		return fmt.Errorf("Column names could not be written to the export file: %v\n", err)
	}

	// collect row data and pass to csv writer
	row := make([][]byte, len(cols))
	rowPtr := make([]any, len(cols))
	for i := range row {
		rowPtr[i] = &row[i]
	}

	for rows.Next() {
		if err := rows.Scan(rowPtr...); err != nil {
			return fmt.Errorf("Unable to properly parse the query result: %v\n", err)
		}
		var values []string
		for _, record := range row {
			values = append(values, string(record))
		}
		if err := w.Write(values); err != nil {
			return fmt.Errorf("Record could not be written to export file: %v\n", err)
		}

	}

	if err := w.Error(); err != nil {
		return fmt.Errorf("Following error occurred while finalizing export file: %v\n", err)
	}

	return nil
}
