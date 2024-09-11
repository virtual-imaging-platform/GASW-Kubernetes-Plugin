####################

VERSION		= 0.0.1
NAME		= gasw-kubernetes-plugin
EXECUTABLE  = target/$(NAME)-$(VERSION)-jar-with-dependencies.jar
EXECUTOR 	= java -jar

####################

SCP_HOST	= 192.168.122.180
SCP_USER	= almalinux
SCP_FOLDER	= /var/www/cgi-bin/m2Server-gasw3/plugins/gasw-local-plugin-3.2.0-jar-with-dependencies.jar

####################

DEBUG_FLAGS = -X

all: compile remote

compile:
	@mvn clean package

debug:
	@mvn clean package $(DEBUG_FLAGS)
	$(MAKE) exec

exec:
	@$(EXECUTOR) $(EXECUTABLE)

remote:
	@scp $(EXECUTABLE) ${SCP_USER}@${SCP_HOST}:$(SCP_FOLDER) 