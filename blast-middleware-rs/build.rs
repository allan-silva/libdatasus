use cc;

fn main() {
    cc::Build::new()
        .file("blast-dbf/blast.c")
        .file("blast-dbf/blast-dbf.c")
        .compile("blast-dbf-x86_64.so");
}
