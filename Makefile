####################

VERSION		= 0.0.1
NAME		= gasw-kubernetes-plugin
EXECUTABLE  = target/$(NAME)-$(VERSION)-jar-with-dependencies.jar
EXECUTOR 	= java -jar

####################

SCP_HOST	= 192.168.122.149
SCP_USER	= centos
SCP_FOLDER	=

####################

DEBUG_FLAGS = -X

all: compile exec

compile:
	@mvn clean package

debug:
	@mvn clean package $(DEBUG_FLAGS)
	$(MAKE) exec

exec:
	@$(EXECUTOR) $(EXECUTABLE)

remote:
	@scp $(EXECUTABLE) ${SCP_USER}@${SCP_HOST}:$(SCP_FOLDER) 