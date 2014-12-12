# Needed because there is a directory `test`
.PHONY: test

GITHUB_URL=https://github.com/keenlabs/capillary

default: compile

compile:
	sbt compile

test: compile
	sbt test
	mkdir test-output
	mv target/test-reports/* test-output

dist:
	sbt universal:package-zip-tarball
	canopy artifact dist target/universal/*.tgz $(GITHUB_URL) $(GIT_COMMIT) $(GIT_BRANCH)

clean:
	rm -rf target
	rm -rf dist
	rm -rf test-output
