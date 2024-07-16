VERSION		= 0.0.1
NAME		= gasw-kubernetes-plugin
EXECUTABLE  = target/$(NAME)-$(VERSION)-jar-with-dependencies.jar
EXECUTOR 	= java -jar

DEBUG_FLAGS = -X

all: compile exec

compile:
	@mvn clean package

debug:
	@mvn clean package $(DEBUG_FLAGS)
	$(MAKE) exec

exec:
	@$(EXECUTOR) $(EXECUTABLE)
