# Needed because there is a directory `test`
.PHONY: test

GITHUB_URL=https://github.com/keenlabs/capillary

default: compile

compile:
	./activator compile

test: compile
	./activator test
	mkdir test-output
	mv target/test-reports/* test-output

dist:
	./activator universal:package-zip-tarball
	canopy dist $(GITHUB_URL) $(GIT_COMMIT) target/universal/*.tgz

clean:
	rm -rf target
	rm -rf dist
	rm -rf test-output
