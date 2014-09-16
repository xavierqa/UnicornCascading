#!/bin/bash +x
ACTION=$1
MESSAGE=$2
SOURCEFOLDER=src/main/java
SOURCERESOURCES=${SOURCEFOLDER}
TESTFOLDER=src/test/java
TESTRESOURCES=${TESTFOLDER}


pushCode(){
	SOURCE=$1
	MESSAGE=$2
	echo "Pushing code ${SOURCE}"
	FILES=`find ${SOURCE} -mtime -1`
	COMMIT=0
	for f in $FILES;
	do 
		GITDIFF=`git diff $f`
		if [ ! -z "$GITDIFF" ]; 
		then
			echo "adding $f to git"
			git add $f
			COMMIT=1
		else
			echo "ZERO $GITDIFF"
		fi	
	done

	echo $MESSAGE
	if [ $COMMIT -gt 0 ];
	then
		echo "COMMITING CODE"
		git commit -m $MESSAGE
		git push -u origin master
	fi
}

pullCode(){
	echo "Pulling code from ${SOURCE}"
	git pull 
}

cloneCode()
{
	REPO=$1
	git clone $REPO 
}


case $ACTION in
	PULL)
		echo "Pulling code from GitHub"
		pullCode
		;;
	PUSH)
		echo "Pushing code from GitHub"
		pushCode $SOURCEFOLDER $MESSAGE
		;;
	CLONE)
		echo "Cloning code from GitHub"
		cloneCode $MESSAGE
		;;	
	*)
		echo "define action"
		;;
esac
	


