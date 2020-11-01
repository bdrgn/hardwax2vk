#!/bin/bash
rm -rf function.zip
zip -r9 function.zip !(.git*) -x "Hardwax2VKLayer.zip"
aws lambda update-function-code --function-name my-function --zip-file fileb://function.zip
rm -rf function.zip