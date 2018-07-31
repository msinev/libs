package dynamopool

import (
"github.com/aws/aws-sdk-go/service/dynamodb"
"github.com/aws/aws-sdk-go/aws"
"log"
"sync"
	"strconv"
)

type DynamoGet struct {
	Table   string
	Key     map[string]string
	WG      *sync.WaitGroup
	Result  map[string]string
}

type DynamoSet struct {
	Table   string
	Set     map[string]string
	TTL 		 int64
	TTLAttribute string
}

var DynamoSetRequest chan<- *DynamoSet

var DynamoGetRequest chan<- *DynamoGet


func InitDynamoIO(svc *dynamodb.DynamoDB) {

	ds:=make (chan *DynamoSet)
	dg:=make (chan *DynamoGet)

	go ProcessGet(dg, svc)
	DynamoGetRequest=dg

	go ProcessSet(ds, svc)
	DynamoSetRequest=ds

}

func GetOrLesser(s *string, l string) string {
	if s==nil {
		return l
	}
	return *s
}

func ProcessGet(inc <-chan *DynamoGet, svc *dynamodb.DynamoDB) {
	for v:=range inc {

		vmap:=make(map[string]*dynamodb.AttributeValue)

		for kl,vl := range v.Key {
			vmap[kl]=&dynamodb.AttributeValue{ S: aws.String(vl) }
		}

		result, err := svc.GetItem(&dynamodb.GetItemInput{
			TableName: aws.String(v.Table),
			Key: vmap,
		})

		if err != nil {
			log.Println("AWS get item %s\n", err.Error())
		}

		rmap:=v.Result
		for rk, rv:= range result.Item {
			rmap[rk]=GetOrLesser(rv.S, GetOrLesser(rv.N, "") );
		}
		if v.WG!=nil {
			v.WG.Done();
		}
	}
}

func ProcessSet(inc <-chan *DynamoSet, svc *dynamodb.DynamoDB) {
	for v:=range inc {

		vmap:=make(map[string]*dynamodb.AttributeValue)
		for kl,vl := range v.Set {
			vmap[kl]=&dynamodb.AttributeValue{
				S: aws.String(vl),
			}
		}

		if v.TTL>0 {
			vmap[v.TTLAttribute]=&dynamodb.AttributeValue{
				N: aws.String(strconv.FormatInt(v.TTL, 10)),
			}
		}

		result, err := svc.PutItem(&dynamodb.PutItemInput{
			TableName: aws.String(v.Table),
			Item: vmap,
		})

		if err != nil {
			log.Println("AWS set item %s\n", err.Error())
		}
		log.Println(result.GoString())
	}
}
