# Needed because there is a directory `test`
.PHONY: test

GITHUB_URL=https://github.com/keenlabs/capillary # XXX: get this from the environment instead

all: compile test dist

compile:
	./activator compile

test:
	./activator test
	mkdir test-output
	mv target/test-reports/* test-output

dist:
	./activator universal:package-zip-tarball
	canopy artifact dist target/universal/*.tgz $(GITHUB_URL) $(GIT_COMMIT) $(GIT_BRANCH)

clean:
	rm -rf target
	rm -rf dist
	rm -rf test-output
