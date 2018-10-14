cd jobs

echo "Creating Jobs package"
sbt package

cd ../

echo "Building Amplifyr Application"
sbt dist

cd target/universal/

if [ -e amplifyr-server-2.0-SNAPSHOT/RUNNING_PID ]
then
    echo "Stopping previous instance"
    fuser -k 9000/tcp

    rm amplifyr-server-2.0-SNAPSHOT/RUNNING_PID
else
   echo "nok"
fi

if [ -e amplifyr-server-2.0-SNAPSHOT.zip ]
then
    echo "Extracting application"

    if unzip amplifyr-server-2.0-SNAPSHOT.zip ; then

        echo "Starting application"

        sleep 1

        amplifyr-server-2.0-SNAPSHOT/bin/amplifyr-server -Dplay.crypto.secret=amplifyr1 -Dconfig.file=./amplifyr-server-2.0-SNAPSHOT/conf/development.conf
    else
        echo "Unzip dist files failed"
    fi

else
   echo "nok"
fi

