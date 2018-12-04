# Docker
DOCKER_CMD = docker
DOCKER_BUILD = $(DOCKER_CMD) build
DOCKER_TAG ?= $(DOCKER_CMD) tag
DOCKER_PUSH ?= $(DOCKER_CMD) push
DOCKER_RUN = $(DOCKER_CMD) run
DOCKER_RMI = $(DOCKER_CMD) rmi -f
DOCKER_EXEC = $(DOCKER_CMD) exec

# escape_docker_tag escape colon char to allow use a docker tag as rule
define escape_docker_tag
$(subst :,--,$(1))
endef

# unescape_docker_tag an escaped docker tag to be use in a docker command
define unescape_docker_tag
$(subst --,:,$(1))
endef

# Docker jupyter image tag
GIT_COMMIT=$(shell git rev-parse HEAD | cut -c1-7)
GIT_DIRTY=
ifneq ($(shell git status --porcelain), )
	GIT_DIRTY := -dirty
endif
DEV_PREFIX := dev
VERSION ?= $(DEV_PREFIX)-$(GIT_COMMIT)$(GIT_DIRTY)

# Docker jupyter image
JUPYTER_IMAGE ?= srcd/gitbase-spark-connector-jupyter
JUPYTER_IMAGE_VERSIONED ?= $(call escape_docker_tag,$(JUPYTER_IMAGE):$(VERSION))

# Versions
SCALA_VERSION ?= 2.11.11
SPARK_VERSION ?= 2.2.1

# if TRAVIS_SCALA_VERSION defined SCALA_VERSION is overrided
ifneq ($(TRAVIS_SCALA_VERSION), )
	SCALA_VERSION := $(TRAVIS_SCALA_VERSION)
endif

# if TRAVIS_TAG defined VERSION is overrided
ifneq ($(TRAVIS_TAG), )
	VERSION := $(TRAVIS_TAG)
endif

# if we are not in master, and it's not a tag the push is disabled
ifneq ($(TRAVIS_BRANCH), master)
	ifeq ($(TRAVIS_TAG), )
        pushdisabled = "push disabled for non-master branches"
	endif
endif

# if this is a pull request, the push is disabled
ifneq ($(TRAVIS_PULL_REQUEST), false)
        pushdisabled = "push disabled for pull-requests"
endif

#SBT
SBT = ./sbt ++$(SCALA_VERSION) -Dspark.version=$(SPARK_VERSION)

# Rules
all: clean build

clean:
	$(SBT) clean

test:
	$(SBT) test

build:
	$(SBT) assembly

travis-test:
	$(SBT) clean coverage test coverageReport scalastyle test:scalastyle

docker-build:
	$(if $(pushdisabled),$(error $(pushdisabled)))

	$(DOCKER_BUILD) -t $(call unescape_docker_tag,$(JUPYTER_IMAGE_VERSIONED)) .

docker-clean:
	$(DOCKER_RMI) $(call unescape_docker_tag,$(JUPYTER_IMAGE_VERSIONED))

docker-push: docker-build
	$(if $(pushdisabled),$(error $(pushdisabled)))

	@if [ "$$DOCKER_USERNAME" != "" ]; then \
		$(DOCKER_CMD) login -u="$$DOCKER_USERNAME" -p="$$DOCKER_PASSWORD"; \
	fi;

	$(DOCKER_PUSH) $(call unescape_docker_tag,$(JUPYTER_IMAGE_VERSIONED))
	@if [ "$$TRAVIS_TAG" != "" ]; then \
		$(DOCKER_TAG) $(call unescape_docker_tag,$(JUPYTER_IMAGE_VERSIONED)) \
			$(call unescape_docker_tag,$(JUPYTER_IMAGE)):latest; \
		$(DOCKER_PUSH) $(call unescape_docker_tag,$(JUPYTER_IMAGE):latest); \
	fi;

maven-release:
	$(SBT) clean publishSigned && \
	$(SBT) sonatypeRelease