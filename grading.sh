#!/bin/bash
if [[ "$OSTYPE" == "linux-gnu"* ]]; then
	EXEC_STR="-executable"
elif [[ "$OSTYPE" == "darwin"* ]]; then
	EXEC_STR="-perm +111"
else
	echo "Platform untested and unsupported"
	exit 1
fi
arg="$3"
if [[ -n "$arg" ]];
then
	arg="-D -f $3"
fi
echo ${arg}
second="$2"
for exno in "$1"
do
    files=$(find tests/tests-$exno -name "test*-*-*" -type f ${EXEC_STR})
    for file in $files
    do
	    timeout -s ABRT $(echo $second)s ./$file $arg
	    if [[ $? == 124 ]]; then
		    echo "FAILURE: The test has timed out after $2 seconds!"
	    fi
	    echo "============================"
    done
done