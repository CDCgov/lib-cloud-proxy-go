package test

import (
	"errors"
	"fmt"
	"github.com/joho/godotenv"
	"lib-cloud-proxy-go/secrets"
)

func init() {
	err := godotenv.Load(".env")
	if err != nil {
		fmt.Println("Unable to load .env")
	}
}
func printCloudSecretsError(err error) {
	if err != nil {
		fmt.Printf("Error occurred: %s \n", err.Error())
		var cloudError *secrets.CloudSecretsError
		if errors.As(err, &cloudError) {
			if cloudError.Unwrap() != nil {
				fmt.Printf("Cloud error caused by: %s \n", cloudError.Unwrap().Error())
			}
		}
	}
}
