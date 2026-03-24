all:
	gcc -o merger merger_skeleton.c merger_parser.c

clean:
	rm -f merger