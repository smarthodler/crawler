package main

import (
	"context"
	"encoding/json"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/xitongsys/parquet-go-source/s3"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
	"io/ioutil"
	"log"
	"net/http"
	"os"
)

var api = os.Getenv("API")

//type Data struct {
//	Datetime string  `json:"datetime" parquet:"name=datetime, type=UTF8"` // Name conversion
//	Values   []int64 `json:"values" parquet:"name=values, type=LIST, valuetype=INT64"`
//}

func DataParquetWriter(data interface{}, bucketUri, fileName string) (*writer.ParquetWriter, source.ParquetFile, error) {
	log.Println("generating parquet file")
	ctx := context.Background()
	fileWriter, err := s3.NewS3FileWriter(ctx, bucketUri, fileName, nil)
	parquetWriter, err := writer.NewParquetWriter(fileWriter, data, int64(1))
	if err != nil {
		return nil, nil, err
	}
	parquetWriter.CompressionType = parquet.CompressionCodec_GZIP

	return parquetWriter, fileWriter, nil
}

func addDataToParquet(pw *writer.ParquetWriter, dataModel interface{}) error {

	return nil
}

func CloseParquetWrite(pw *writer.ParquetWriter, fw source.ParquetFile) error {
	if err := pw.WriteStop(); err != nil {
		return err
	}

	err := fw.Close()
	if err != nil {
		return err
	}
	return nil
}

func Crawl(apiUrl, bucketUri string, fileName string, dataModel interface{}) error {

	retryClient := retryablehttp.NewClient()
	retryClient.RetryMax = 3
	client := retryClient.StandardClient()
	req, err := http.NewRequest("GET", apiUrl, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	bodyText, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	err = json.Unmarshal(bodyText, dataModel)
	if err != nil {
		log.Fatal(err)
	}

	parquetWriter, fileWriter, err := DataParquetWriter(dataModel, bucketUri, fileName)
	if err != nil {
		return err
	}

	if err := parquetWriter.Write(dataModel); err != nil {
		return err
	}

	err = CloseParquetWrite(parquetWriter, fileWriter)

	if err != nil {
		return err
	}

	return nil
}
