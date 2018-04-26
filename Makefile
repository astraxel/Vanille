OCAMLBUILD=ocamlbuild -classic-display \
		-tags annot,debug,thread \
		-libs unix
TARGET=native

example:
	$(OCAMLBUILD) example.$(TARGET)
	$(OCAMLBUILD) exampleUnix.$(TARGET)
	$(OCAMLBUILD) exampleSeq.$(TARGET)


clean:
	$(OCAMLBUILD) -clean

realclean: clean
	rm -f *~

cleanall: realclean
