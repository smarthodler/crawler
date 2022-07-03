// savedata contains simple function for getting a data from an API and writing it to an S3 file.
// no tests, yolo
// Call SaveData in a lambda to scrape exchange API.
package savedata

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/hashicorp/go-retryablehttp"
	"github.com/xitongsys/parquet-go-source/s3"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
)

func parquetFileWriter(data interface{}, bucketUri, fileName string) (*writer.ParquetWriter, source.ParquetFile, error) {
	fileWriter, err := s3.NewS3FileWriter(context.Background(), bucketUri, fileName, nil)
	parquetWriter, err := writer.NewParquetWriter(fileWriter, data, int64(1))
	if err != nil {
		return nil, nil, err
	}
	parquetWriter.CompressionType = parquet.CompressionCodec_GZIP

	return parquetWriter, fileWriter, nil
}

func closeWriter(pw *writer.ParquetWriter, fw source.ParquetFile) error {
	if err := pw.WriteStop(); err != nil {
		return err
	}
	if err := fw.Close(); err != nil {
		return err
	}

	return nil
}

func SaveData(apiUrl, bucketUri string, fileName string, dataModel interface{}) error {

	client := retryablehttp.NewClient().StandardClient()
	req, err := http.NewRequest("GET", apiUrl, nil)
	if err != nil {
		return err
	}

	log.Println("Get data.")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	bodyText, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	log.Println("Unmarshall response body.")
	if err = json.Unmarshal(bodyText, dataModel); err != nil {
		return err
	}

	log.Println("Get parquet file writer.")
	parquetWriter, fileWriter, err := parquetFileWriter(dataModel, bucketUri, fileName)
	if err != nil {
		return err
	}

	log.Println("Write to S3 bucket.")
	if err := parquetWriter.Write(dataModel); err != nil {
		return err
	}

	return closeParquetWrite(parquetWriter, fileWriter)
}
