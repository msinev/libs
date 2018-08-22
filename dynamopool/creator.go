package dynamopool

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"log"
	"os"
	"time"
)

func CreateTablesFromSchema(dbSvc *dynamodb.DynamoDB, input []*dynamodb.CreateTableInput, TTLcolumn map[string]string) {

	result, err := dbSvc.ListTables(&dynamodb.ListTablesInput{})
	if err != nil {
		log.Println(err)
		return
	}

	exist := make(map[string]bool)

	for _, v := range input {
		exist[*v.TableName] = false
	}

	//	  log.Println("Tables:")
	//	  log.Println(*table)

	for _, table := range result.TableNames {
		if _, ok := exist[*table]; ok {
			log.Printf("Table %s already exists", *table)
			exist[*table] = true
		}
	}

	for _, v := range input {
		tableName := *v.TableName
		if !exist[tableName] {
			log.Printf("Creating table %s", tableName)
			_, err := dbSvc.CreateTable(v)

			if err != nil {
				log.Printf("Got error calling CreateTable: %s\n", tableName)
				log.Println(err.Error())
				os.Exit(1)
			}
		}
		TTLcolumnName, haveTTL := TTLcolumn[tableName]

		if haveTTL {
			if !exist[tableName] {
				log.Printf("Wait 20seconds after creating table %s for TTL to set up", tableName)
				time.Sleep(time.Second * 20)
			}
			checkTableTTL(dbSvc, *v.TableName, TTLcolumnName)
		}
	}
	// Existing
}
func sDef(s *string, def string) string {
	if s == nil {
		return def
	}
	return *s
}

func checkTableTTL(dbSvc *dynamodb.DynamoDB, table string, TTLcolumn string) {

	log.Printf("Checking TTL column %s for table %s", TTLcolumn, table)

	ttli := dynamodb.DescribeTimeToLiveInput{TableName: aws.String(table)}
	//var rttl *dynamodb.DescribeTimeToLiveOutput

	rttl, err := dbSvc.DescribeTimeToLive(&ttli)
	if err != nil {
		log.Printf("Got error calling DescribeTimeToLive: %s", table)
		log.Println(err.Error())
		os.Exit(1)
	}
	log.Printf("Current ttl-status for table\"%s\" is \"%s\" for attrubute \"%s\"", table, sDef(rttl.TimeToLiveDescription.TimeToLiveStatus, "-nil-"), sDef(rttl.TimeToLiveDescription.AttributeName, "-nil-"))

	if *rttl.TimeToLiveDescription.TimeToLiveStatus != dynamodb.TimeToLiveStatusEnabled {
		log.Printf("Enabling TTL on table %s", table)
		sttli := dynamodb.UpdateTimeToLiveInput{TableName: aws.String(table), TimeToLiveSpecification: &dynamodb.TimeToLiveSpecification{
			AttributeName: aws.String(TTLcolumn),
			Enabled:       aws.Bool(true),
		}}

		//		var ruttl *dynamodb.UpdateTimeToLiveOutput

		_, err = dbSvc.UpdateTimeToLive(&sttli)
		if err != nil {
			log.Printf("Got error calling UpdateTimeToLive: %s/%s", table, TTLcolumn)
			log.Println(err.Error())
			os.Exit(1)
		} else {
			log.Printf("Success calling UpdateTimeToLive: %s/%s", table, TTLcolumn)
		}
	}

	log.Printf("Checking table %s done", table)
}
