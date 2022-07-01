.phony: test
test: build test.o
	g++ test.o xxhash.o consistent_hasing.o -o run.out
	./run.out

.phony: build
build: xxhash.o consistent_hasing.o

%.o: %.c %.h
	g++ -c $< -o $@

%.o: %.cc %.h
	g++ -c $< -o $@

.phony: clean
clean:
	rm -rf *.o *.out