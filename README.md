**Requirements for running test cases:**

1) Install sbt on your system:
```
echo "deb https://dl.bintray.com/sbt/debian /" | sudo tee -a /etc/apt/sources.list.d/sbt.list
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823
sudo apt-get update
sudo apt-get install sbt
```

2) Install awscli on your system and set the credentials for aws command line access [Enter the aws credentials when prompted]:
```
sudo pip install awscli
aws configure
```
3) Update the user access token in application.test.conf
```
authconfig.email = "xxxxxx@xxxxx.com"
authconfig.test_token = ya29.GlxLBPLx3IvWp7PEgKMQaR_1Yc_TXXXXXXX
```

4) Within the amplifyr-server folder, run tests using:
```
sbt test
```

5) Run tests for checking file webservices:
```
sbt "test-only FileHandlerSpec"
```

6) Run tests for checking only acl:
```
sbt "test-only AccessControlListSpec"
```

**Running Swagger Editor:**

1) Install the swagger module:
```
npm install -g swagger

```
Create a new swagger project
```
swagger project create project_name

```

3) Design your API in the Swagger Editor:
```
swagger project edit
```

