# #!/bin/bash
# # set -x
# if [ -f ./tests/run-tests.sh ]; then
#   source ./tests/run-tests.sh
#   RESULT=$?
#   if [ ! -z "${FILE_LOCATION}"]; then
#     if [ ${RESULT} -ne 0 ]; then STATUS=fail; else STATUS=pass; fi
#       if jq -e '.services[] | select(.service_id=="draservicebroker")' _toolchain.json; then
#         ibmcloud login --apikey ${IBM_CLOUD_API_KEY} --no-region
#         ibmcloud doi publishtestrecord --type unittest --buildnumber ${BUILD_NUMBER} --filelocation ${FILE_LOCATION} \
#           --buildnumber ${BUILD_NUMBER} --logicalappname ${IMAGE_NAME} --status ${STATUS}
#       fi
#     exit $RESULT
#   fi
# else
#   echo "Test runner script not found: ./tests/run-tests.sh"
# fi